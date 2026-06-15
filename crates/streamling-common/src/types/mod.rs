pub mod decimal_arb;
pub mod decimal_arb_capability;
// Feature 002 (Retire U256/I256): the `u256` and `i256` modules have been
// removed. Wide integers flow through `decimal_arb` with the optional
// `native_int_kind` hint. See `specs/002-retire-u256-i256/`.
