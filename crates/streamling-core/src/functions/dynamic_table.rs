use crate::dynamic_table::{DynamicTableBackend, DynamicTableRegistry};
use crate::error::{ResultExt, StreamlingError};
use crate::{streamling_user_bail, streamling_user_err};
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Boolean;
use datafusion::arrow::datatypes::DataType::Utf8;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tracing::debug;

#[derive(Debug)]
pub struct DynamicTableCheckFunc {
    registry: DynamicTableRegistry,
    signature: Signature,
}

// `DynamicTableRegistry` wraps runtime state (`Arc<RwLock<HashMap<..>>>`) that is
// not comparable, so UDF identity (required by df54's `ScalarUDFImpl: DynEq + DynHash`)
// is defined purely by the signature.
impl PartialEq for DynamicTableCheckFunc {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}
impl Eq for DynamicTableCheckFunc {}
impl std::hash::Hash for DynamicTableCheckFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl DynamicTableCheckFunc {
    pub fn new(registry: DynamicTableRegistry) -> Self {
        Self {
            registry,
            signature: Signature::new(
                // dynamic table name, value
                TypeSignature::Exact(vec![Utf8, Utf8]),
                Volatility::Volatile,
            ),
        }
    }

    fn get_backend(&self, name: &str) -> Result<Arc<dyn DynamicTableBackend>> {
        let backend = self.registry.get(name)?.ok_or_else(|| {
            streamling_user_err!("No dynamic table backend found for name '{}'", name)
        })?;
        Ok(backend)
    }

    /// Validate that all table names in the array are the same
    fn validate_table_names_and_extract_first(
        &self,
        table_names_arr: &StringArray,
    ) -> Result<String> {
        if table_names_arr.is_empty() {
            streamling_user_bail!("Table names array cannot be empty");
        }

        if table_names_arr.is_null(0) {
            streamling_user_bail!("Table name cannot be null");
        }

        let first_table_name = table_names_arr.value(0);

        // Validate all table names are the same
        for i in 1..table_names_arr.len() {
            if !table_names_arr.is_null(i) && table_names_arr.value(i) != first_table_name {
                streamling_user_bail!(
                    "All table names must be the same. Found '{}' and '{}'",
                    first_table_name,
                    table_names_arr.value(i)
                );
            }
        }

        Ok(String::from(first_table_name))
    }

    fn execute(
        &self,
        table_names: &ColumnarValue,
        values: &ColumnarValue,
    ) -> Result<ColumnarValue> {
        let mut table_name: Option<String> = None;

        if let ColumnarValue::Array(tn_arr) = table_names
            && let Some(table_names_arr) = tn_arr.as_any().downcast_ref::<StringArray>()
        {
            table_name = Some(self.validate_table_names_and_extract_first(table_names_arr)?);
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8(value)) = table_names {
            table_name = value.clone();
        }

        if table_name.is_none() {
            streamling_user_bail!(
                "dynamic_table_check requires string array or string scalar as first argument"
            );
        }
        let table_name = table_name.unwrap();

        if let ColumnarValue::Array(v_arr) = values
            && let Some(values_arr) = v_arr.as_any().downcast_ref::<StringArray>()
        {
            let backend = self.get_backend(table_name.as_str())?;

            debug!(
                "Using dynamic table backend for table '{}' with {} values",
                table_name,
                values_arr.len()
            );

            let values_array_ref: ArrayRef = Arc::clone(v_arr);

            // Block on the async backend.contains() call
            // Prefer an existing Tokio runtime; otherwise fall back to a simple block_on
            let result_array_ref = match Handle::try_current() {
                Ok(handle) => {
                    // Use block_in_place to avoid blocking the entire Tokio runtime
                    tokio::task::block_in_place(|| {
                        handle.block_on(backend.contains(values_array_ref))
                    })
                }
                Err(_) => {
                    // If no Tokio runtime is present, create a temporary one to block on the future
                    let rt = Runtime::new().streamling_context("failed to create Tokio runtime")?;
                    rt.block_on(backend.contains(values_array_ref))
                }
            }
            .map_err(StreamlingError::from)
            .streamling_context(format!("failed to check dynamic table '{}'", table_name))?;

            // The result is already a BooleanArray wrapped in ArrayRef
            return Ok(ColumnarValue::Array(result_array_ref));
        }

        Err(streamling_user_err!("dynamic_table_check requires string array arguments").into())
    }
}

impl ScalarUDFImpl for DynamicTableCheckFunc {
    fn name(&self) -> &str {
        "dynamic_table_check"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            streamling_user_bail!("dynamic_table_check requires exactly two arguments");
        }

        self.execute(&args.args[0], &args.args[1])
    }
}
