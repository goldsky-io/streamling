pub mod decimal_arb;
pub mod decimal_arb_capability;
pub mod decimal_arb_legacy;
// The `u256` and `i256` modules have been removed. Wide integers flow
// through `decimal_arb` with the optional `native_int_kind` hint.
