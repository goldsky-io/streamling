use crate::functions::array_enumerate::ArrayEnumerateFunc;
use crate::functions::array_filter::ArrayFilterFunc;
use crate::functions::array_filter_first::ArrayFilterFirstFunc;
use crate::functions::array_filter_in::ArrayFilterInFunc;
use crate::functions::byte_reverse::ReverseBytes32Func;
use crate::functions::byte_to_hex::ByteToHexFunc;
use crate::functions::coalesce_meta::CoalesceMetaUdf;
use crate::functions::conv_base::ConvBaseFunc;
use crate::functions::current_date::VolatileCurrentDateFunc;
use crate::functions::current_time::VolatileCurrentTimeFunc;
use crate::functions::decimal_arb_ops::{
    DecimalArbAbsFunc, DecimalArbAddFunc, DecimalArbDivFunc, DecimalArbEqFunc, DecimalArbGtFunc,
    DecimalArbGteFunc, DecimalArbLtFunc, DecimalArbLteFunc, DecimalArbModFunc, DecimalArbMulFunc,
    DecimalArbNegFunc, DecimalArbNeqFunc, DecimalArbSortKeyFunc, DecimalArbSubFunc,
    DecimalArbToDecimal128Func, DecimalArbToDecimal256Func, DecimalArbToStringFunc,
    ToDecimalArbFromDecimal128Func, ToDecimalArbFromDecimal256Func, ToDecimalArbFromIntFunc,
    ToDecimalArbFromStringFunc,
};
use crate::functions::from_base58::create_from_base58_udf;
use crate::functions::generate_series::GenerateSeriesFunc;
use crate::functions::gs_map_to_array_struct::create_gs_map_to_array_struct_udf;
use crate::functions::hex_to_byte::HexToByteFunc;
// Feature 002 (Retire U256/I256): u256_ops / i256_ops UDFs removed. Wide
// integers now flow through decimal_arb's UDF surface (decimal_arb_add,
// decimal_arb_to_string, etc., registered below).
use crate::functions::json_objects_to_clickhouse_tuples::JsonObjectsToClickhouseTuplesFunc;
use crate::functions::keccak256::Keccak256Func;
use crate::functions::now::VolatileNowFunc;
use crate::functions::split_string_to_array::SplitStringToArrayFunc;
use crate::functions::to_large_list::ToLargeListFunc;
use crate::functions::uuid7::Uuid7Func;
use crate::functions::xxhash::XxHashFunc;
use crate::functions::zip_arrays::ZipArraysFunc;
use datafusion::logical_expr::ScalarUDF;

pub mod array_enumerate;
pub mod array_filter;
pub mod array_filter_first;
pub mod array_filter_in;
pub mod byte_reverse;
pub mod byte_to_hex;
pub mod coalesce_meta;
pub mod conv_base;
pub mod current_date;
pub mod current_time;
pub mod decimal_arb_aggregates;
pub mod decimal_arb_coercion;
pub mod decimal_arb_ops;
pub mod decimal_arb_sort_optimizer;
pub mod from_base58;
pub mod generate_series;
pub mod gs_map_to_array_struct;
pub mod hex_to_byte;
// Feature 002: i256_ops module deleted alongside the I256Type retirement.
pub mod json_objects_to_clickhouse_tuples;
pub mod json_string;
pub mod keccak256;
pub mod now;
pub mod split_string_to_array;
pub mod to_large_list;
// Feature 002: u256_ops module deleted alongside the U256Type retirement.
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
            ScalarUDF::from(ToLargeListFunc::new()),
            ScalarUDF::from(XxHashFunc::new()),
            ScalarUDF::from(Keccak256Func::new()),
            ScalarUDF::from(ConvBaseFunc::new()),
            ScalarUDF::from(CoalesceMetaUdf::new()),
            ScalarUDF::from(json_string::JsonStringFunc::new()),
            create_from_base58_udf(),
            ScalarUDF::from(HexToByteFunc::new()),
            ScalarUDF::from(ByteToHexFunc::new()),
            ScalarUDF::from(ReverseBytes32Func::new()),
            create_gs_map_to_array_struct_udf(),
            // U256/I256 UDFs retired in feature 002. Wide-int arithmetic
            // and conversion now happens through the decimal_arb UDFs below.
            ScalarUDF::from(Uuid7Func::new()),
            // decimal_arb functions (US1 sink-side helper + US2 arithmetic)
            ScalarUDF::from(DecimalArbToStringFunc::new()),
            ScalarUDF::from(DecimalArbAddFunc::new()),
            ScalarUDF::from(DecimalArbSubFunc::new()),
            ScalarUDF::from(DecimalArbMulFunc::new()),
            ScalarUDF::from(DecimalArbDivFunc::new()),
            ScalarUDF::from(DecimalArbModFunc::new()),
            ScalarUDF::from(DecimalArbNegFunc::new()),
            ScalarUDF::from(DecimalArbAbsFunc::new()),
            // decimal_arb comparisons (US2 / T043)
            ScalarUDF::from(DecimalArbEqFunc::new()),
            ScalarUDF::from(DecimalArbNeqFunc::new()),
            ScalarUDF::from(DecimalArbLtFunc::new()),
            ScalarUDF::from(DecimalArbLteFunc::new()),
            ScalarUDF::from(DecimalArbGtFunc::new()),
            ScalarUDF::from(DecimalArbGteFunc::new()),
            // decimal_arb sort-key helper (US2 / T046 partial)
            ScalarUDF::from(DecimalArbSortKeyFunc::new()),
            // decimal_arb cast UDFs (US4 / T068)
            ScalarUDF::from(ToDecimalArbFromStringFunc::new()),
            ScalarUDF::from(ToDecimalArbFromDecimal128Func::new()),
            ScalarUDF::from(ToDecimalArbFromDecimal256Func::new()),
            ScalarUDF::from(DecimalArbToDecimal128Func::new()),
            ScalarUDF::from(DecimalArbToDecimal256Func::new()),
            ScalarUDF::from(ToDecimalArbFromIntFunc::new()),
        ]
    }
}
