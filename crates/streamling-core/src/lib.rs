// Re-exported from streamling-common
pub use streamling_common::data;
pub use streamling_common::error;
pub use streamling_common::formats;

// Re-export macros so existing `use crate::streamling_err` / `streamling_core::streamling_err` work
pub use streamling_common::{
    streamling_bail, streamling_err, streamling_retriable_err, streamling_user_bail,
    streamling_user_err,
};

// These modules re-export from common and add core-specific submodules
pub mod types;
pub mod utils;

pub mod admin_api;
pub mod app_config;
pub mod checkpoints;
pub mod dynamic_table;
pub mod functions;
pub mod node_context;
pub mod operators;
pub mod optimizer;
pub mod plugin;
pub mod retry;
pub mod schema;
pub mod serde;
pub mod session;
pub mod shutdown;
pub mod side_output;
pub mod sql_parse;
pub mod telemetry;
pub mod topology;
pub mod topology_validation;

#[cfg(test)]
mod tests {}
