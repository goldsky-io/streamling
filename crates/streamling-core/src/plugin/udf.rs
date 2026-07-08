use abi_stable::std_types::{RResult, RString, RVec};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use lazy_static::lazy_static;
use std::sync::{Arc, RwLock};
use streamling_plugin::{PluginUdfDescriptor, SafeArrowColumn, SafeUdfArg};
use tracing::info;

lazy_static! {
    static ref PLUGIN_UDF_DESCRIPTORS: RwLock<Vec<StoredUdfDescriptor>> = RwLock::new(Vec::new());
}

/// Stored version of PluginUdfDescriptor with pre-converted types.
struct StoredUdfDescriptor {
    name: String,
    aliases: Vec<String>,
    type_signatures: Vec<Vec<DataType>>,
    return_type: DataType,
    deterministic: bool,
    invoke: extern "C" fn(
        args: RVec<SafeUdfArg>,
        number_rows: usize,
    ) -> RResult<SafeArrowColumn, RString>,
}

pub fn store_plugin_udfs(descriptors: RVec<PluginUdfDescriptor>) {
    let mut registry = PLUGIN_UDF_DESCRIPTORS.write().unwrap();
    for descriptor in descriptors {
        let name = descriptor.name.to_string();
        let aliases: Vec<String> = descriptor
            .aliases
            .into_iter()
            .map(|a| a.to_string())
            .collect();
        let type_signatures: Vec<Vec<DataType>> = descriptor
            .type_signatures
            .into_iter()
            .map(|sig| sig.into_iter().map(DataType::from).collect())
            .collect();
        let return_type = DataType::from(descriptor.return_type);
        info!("Registered plugin UDF: {} (aliases: {:?})", name, aliases);
        registry.push(StoredUdfDescriptor {
            name,
            aliases,
            type_signatures,
            return_type,
            deterministic: descriptor.deterministic,
            invoke: descriptor.invoke,
        });
    }
}

pub fn register_plugin_udfs(ctx: &datafusion::prelude::SessionContext) {
    let registry = PLUGIN_UDF_DESCRIPTORS.read().unwrap();
    for descriptor in registry.iter() {
        let udf = PluginScalarUdf::new(
            descriptor.name.clone(),
            descriptor.aliases.clone(),
            descriptor.type_signatures.clone(),
            descriptor.return_type.clone(),
            descriptor.deterministic,
            descriptor.invoke,
        );
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(udf));
    }
}

/// Host-side adapter that wraps a plugin UDF descriptor as a DataFusion `ScalarUDFImpl`.
#[derive(Debug)]
struct PluginScalarUdf {
    name: String,
    aliases: Vec<String>,
    signature: Signature,
    return_type: DataType,
    invoke_fn: extern "C" fn(
        args: RVec<SafeUdfArg>,
        number_rows: usize,
    ) -> RResult<SafeArrowColumn, RString>,
}

// UDF identity (required by df54's `ScalarUDFImpl: DynEq + DynHash`) is defined by the
// descriptor fields. `invoke_fn` is excluded — comparing function pointers is not
// meaningful (addresses are not guaranteed unique) and a plugin UDF is identified by
// its name/signature/return type.
impl PartialEq for PluginScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.aliases == other.aliases
            && self.signature == other.signature
            && self.return_type == other.return_type
    }
}
impl Eq for PluginScalarUdf {}
impl std::hash::Hash for PluginScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.aliases.hash(state);
        self.signature.hash(state);
        self.return_type.hash(state);
    }
}

impl PluginScalarUdf {
    fn new(
        name: String,
        aliases: Vec<String>,
        type_signatures: Vec<Vec<DataType>>,
        return_type: DataType,
        deterministic: bool,
        invoke_fn: extern "C" fn(
            args: RVec<SafeUdfArg>,
            number_rows: usize,
        ) -> RResult<SafeArrowColumn, RString>,
    ) -> Self {
        let volatility = if deterministic {
            Volatility::Immutable
        } else {
            Volatility::Volatile
        };
        let signature = if type_signatures.len() == 1 {
            Signature::exact(type_signatures.into_iter().next().unwrap(), volatility)
        } else {
            Signature::one_of(
                type_signatures
                    .into_iter()
                    .map(TypeSignature::Exact)
                    .collect(),
                volatility,
            )
        };
        Self {
            name,
            aliases,
            signature,
            return_type,
            invoke_fn,
        }
    }
}

impl ScalarUDFImpl for PluginScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<arrow::datatypes::FieldRef> {
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            self.return_type.clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let ffi_args: RVec<SafeUdfArg> = args
            .args
            .into_iter()
            .map(|cv| match cv {
                ColumnarValue::Array(arr) => Ok(SafeUdfArg {
                    column: SafeArrowColumn::from(arr),
                    is_scalar: false,
                }),
                ColumnarValue::Scalar(s) => {
                    let arr = s.to_array_of_size(1)?;
                    Ok(SafeUdfArg {
                        column: SafeArrowColumn::from(arr),
                        is_scalar: true,
                    })
                }
            })
            .collect::<Result<RVec<_>>>()?;

        match (self.invoke_fn)(ffi_args, number_rows) {
            RResult::ROk(result) => {
                let array = arrow::array::ArrayRef::from(result);
                Ok(ColumnarValue::Array(array))
            }
            RResult::RErr(msg) => Err(crate::streamling_err!("plugin UDF error: {}", msg).into()),
        }
    }
}
