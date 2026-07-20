//! Adversarial regression suite for the arrow-avro decode migration (PR #60).
//!
//! Ten areas, each authored to catch drift from the removed vendored decoder. Combined into a
//! single test binary (via `#[path]` module includes) so the suite links once instead of ten
//! times — ten statically-linked DataFusion test binaries otherwise exhaust the sandbox disk.
#[path = "adversarial/adv_01_u256_i256.rs"]
mod adv_01_u256_i256;
#[path = "adversarial/adv_02_scaled_decimal_string.rs"]
mod adv_02_scaled_decimal_string;
#[path = "adversarial/adv_03_standard_decimal.rs"]
mod adv_03_standard_decimal;
#[path = "adversarial/adv_04_framing_malformed.rs"]
mod adv_04_framing_malformed;
#[path = "adversarial/adv_05_union_root_debezium.rs"]
mod adv_05_union_root_debezium;
#[path = "adversarial/adv_06_schema_evolution.rs"]
mod adv_06_schema_evolution;
#[path = "adversarial/adv_07_skip_schema_resolution.rs"]
mod adv_07_skip_schema_resolution;
#[path = "adversarial/adv_08_nested_containers.rs"]
mod adv_08_nested_containers;
#[path = "adversarial/adv_09_type_mapping_fidelity.rs"]
mod adv_09_type_mapping_fidelity;
#[path = "adversarial/adv_10_aliasing_and_integration.rs"]
mod adv_10_aliasing_and_integration;
