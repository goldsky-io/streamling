//! Resource managers for creating and cleaning up isolated test resources.

mod clickhouse;
mod external_handler;
mod kafka;
pub mod mysql;
mod postgres;
mod print_sink;
mod prometheus;
mod sqs;
mod webhook;

pub use clickhouse::ClickHouseResource;
pub use external_handler::{CapturedHandlerRequest, ExternalHandlerResource};
pub use kafka::KafkaResource;
pub use mysql::MySqlResource;
pub use postgres::PostgresResource;
pub use print_sink::{PrintSinkOutput, PrintSinkRow};
pub use prometheus::PrometheusResource;
pub use sqs::SqsResource;
pub use webhook::WebhookResource;
