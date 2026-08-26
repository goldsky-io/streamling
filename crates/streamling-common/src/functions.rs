use crate::functions::array_enumerate::ArrayEnumerateFunc;
use crate::functions::array_filter::ArrayFilterFunc;
use crate::functions::array_filter_first::ArrayFilterFirstFunc;
use crate::functions::array_filter_in::ArrayFilterInFunc;
use crate::functions::array_struct_field::ArrayStructFieldFunc;
use crate::functions::byte_reverse::ReverseBytes32Func;
use crate::functions::byte_to_hex::ByteToHexFunc;
use crate::functions::coalesce_meta::CoalesceMetaUdf;
use crate::functions::conv_base::ConvBaseFunc;
use crate::functions::current_date::VolatileCurrentDateFunc;
use crate::functions::current_time::VolatileCurrentTimeFunc;
use crate::functions::from_base58::create_from_base58_udf;
use crate::functions::generate_series::GenerateSeriesFunc;
use crate::functions::gs_map_to_array_struct::create_gs_map_to_array_struct_udf;
use crate::functions::hex_to_byte::HexToByteFunc;
use crate::functions::i256_ops::{
    I256AbsFunc, I256AddFunc, I256DivFunc, I256ModFunc, I256MulFunc, I256NegFunc, I256SubFunc,
    I256ToStringFunc, ToI256Func, ToInt64Func,
};
use crate::functions::json_objects_to_clickhouse_tuples::JsonObjectsToClickhouseTuplesFunc;
use crate::functions::keccak256::Keccak256Func;
use crate::functions::now::VolatileNowFunc;
use crate::functions::split_string_to_array::SplitStringToArrayFunc;
use crate::functions::to_base58::ToBase58Func;
use crate::functions::to_large_list::ToLargeListFunc;
use crate::functions::u256_ops::{
    ToU256Func, U256AddFunc, U256DivFunc, U256ModFunc, U256MulFunc, U256SubFunc, U256ToStringFunc,
};
use crate::functions::uuid7::Uuid7Func;
use crate::functions::xxhash::XxHashFunc;
use crate::functions::zip_arrays::ZipArraysFunc;
use datafusion::logical_expr::ScalarUDF;

pub mod array_enumerate;
pub mod array_filter;
pub mod array_filter_first;
pub mod array_filter_in;
pub mod array_struct_field;
pub mod byte_reverse;
pub mod byte_to_hex;
pub mod coalesce_meta;
pub mod conv_base;
pub mod current_date;
pub mod current_time;
pub mod from_base58;
pub mod generate_series;
pub mod gs_map_to_array_struct;
pub mod hex_to_byte;
pub mod i256_ops;
pub mod json_objects_to_clickhouse_tuples;
pub mod json_string;
pub mod keccak256;
pub mod now;
pub mod split_string_to_array;
pub mod to_base58;
pub mod to_large_list;
pub mod u256_ops;
pub mod util;
pub mod uuid7;
pub mod xxhash;
pub mod zip_arrays;

pub struct CommonFunctions;

impl CommonFunctions {
    pub fn functions() -> Vec<ScalarUDF> {
        vec![
            ScalarUDF::from(VolatileNowFunc::new()),
            ScalarUDF::from(VolatileCurrentTimeFunc::new()),
            ScalarUDF::from(VolatileCurrentDateFunc::new()),
            ScalarUDF::from(JsonObjectsToClickhouseTuplesFunc::new()),
            ScalarUDF::from(SplitStringToArrayFunc::new()),
            ScalarUDF::from(GenerateSeriesFunc::new()),
            ScalarUDF::from(ZipArraysFunc::new()),
            ScalarUDF::from(ArrayEnumerateFunc::new()),
            ScalarUDF::from(ArrayFilterFunc::new()),
            ScalarUDF::from(ArrayFilterFirstFunc::new()),
            ScalarUDF::from(ArrayFilterInFunc::new()),
            ScalarUDF::from(ArrayStructFieldFunc::new()),
            ScalarUDF::from(ToLargeListFunc::new()),
            ScalarUDF::from(XxHashFunc::new()),
            ScalarUDF::from(Keccak256Func::new()),
            ScalarUDF::from(ConvBaseFunc::new()),
            ScalarUDF::from(CoalesceMetaUdf::new()),
            ScalarUDF::from(json_string::JsonStringFunc::new()),
            create_from_base58_udf(),
            ScalarUDF::from(HexToByteFunc::new()),
            ScalarUDF::from(ByteToHexFunc::new()),
            ScalarUDF::from(ToBase58Func::new()),
            ScalarUDF::from(ReverseBytes32Func::new()),
            create_gs_map_to_array_struct_udf(),
            // U256 functions
            ScalarUDF::from(ToU256Func::new()),
            ScalarUDF::from(U256ToStringFunc::new()),
            ScalarUDF::from(U256AddFunc::new()),
            ScalarUDF::from(U256SubFunc::new()),
            ScalarUDF::from(U256MulFunc::new()),
            ScalarUDF::from(U256DivFunc::new()),
            ScalarUDF::from(U256ModFunc::new()),
            // I256 functions
            ScalarUDF::from(ToI256Func::new()),
            ScalarUDF::from(I256ToStringFunc::new()),
            ScalarUDF::from(I256AddFunc::new()),
            ScalarUDF::from(I256SubFunc::new()),
            ScalarUDF::from(I256MulFunc::new()),
            ScalarUDF::from(I256DivFunc::new()),
            ScalarUDF::from(I256ModFunc::new()),
            ScalarUDF::from(I256NegFunc::new()),
            ScalarUDF::from(I256AbsFunc::new()),
            ScalarUDF::from(ToInt64Func::new()),
            ScalarUDF::from(Uuid7Func::new()),
        ]
    }
}
