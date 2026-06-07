use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::fmt::format::{DefaultFields, Format};

pub fn create_logger() -> SubscriberBuilder<DefaultFields, Format, EnvFilter> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env())
}
