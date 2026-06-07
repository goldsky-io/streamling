use async_trait::async_trait;
use in_memory::InMemoryStateOperatorBackendFactory;
use postgres::PostgresStateOperatorBackendFactory;
use serde::{Deserialize, Serialize};
use sqlite::SqliteStateOperatorBackendFactory;
use std::convert::From;
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use streamling_config::app_config::{StateBackendConfig, StateBackendType};

pub mod in_memory;
pub mod postgres;
pub mod sqlite;

#[cfg(feature = "test-utils")]
pub mod testing;

/// Type for the keys used in the state backend.
/// For simplicity, it's assumed that all state backends will use the same key format (strings).
/// If it were to change, a new generic type could be added, similar to the `V` type parameter.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StateKey(pub String);

impl From<&str> for StateKey {
    fn from(s: &str) -> Self {
        StateKey(s.to_string())
    }
}

impl From<String> for StateKey {
    fn from(s: String) -> Self {
        StateKey(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateBackendErrorKind {
    Initialization,
    Connection,
    Query,
    Serialization,
}

#[derive(Debug)]
pub struct StateBackendError {
    kind: StateBackendErrorKind,
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl StateBackendError {
    pub fn new<M: Into<String>>(kind: StateBackendErrorKind, message: M) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source<M, E>(kind: StateBackendErrorKind, message: M, source: E) -> Self
    where
        M: Into<String>,
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn kind(&self) -> StateBackendErrorKind {
        self.kind
    }
}

impl fmt::Display for StateBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)?;
        if let Some(source) = &self.source {
            write!(f, "\n\nCaused by:\n    {}", source)?;
        }
        Ok(())
    }
}

impl StdError for StateBackendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn StdError + 'static))
    }
}

#[async_trait]
pub trait StateOperatorBackend<V>: Debug + Sync + Send
where
    V: Serialize + for<'de> Deserialize<'de>,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError>;
    async fn put(&self, key: StateKey, value: V) -> Result<(), StateBackendError>;
    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError>;
    async fn clear(&self) -> Result<(), StateBackendError>;
}

pub enum StateBackendFactories {
    InMemory(InMemoryStateOperatorBackendFactory),
    Postgres(PostgresStateOperatorBackendFactory),
    Sqlite(SqliteStateOperatorBackendFactory),
}

impl StateBackendFactories {
    pub fn new(config: StateBackendConfig) -> Result<Self, StateBackendError> {
        let init_future = async move {
            match config.backend_type {
                StateBackendType::InMemory => Ok(StateBackendFactories::InMemory(
                    InMemoryStateOperatorBackendFactory::new()?,
                )),
                StateBackendType::Postgres => {
                    let postgres_config = config
                        .postgres
                        .expect("Postgres JSON backend config is required");
                    Ok(StateBackendFactories::Postgres(
                        PostgresStateOperatorBackendFactory::new(
                            postgres_config.connection_url(),
                            postgres_config.max_connections,
                            postgres_config.state_schema_name,
                            postgres_config.state_table_name,
                        )
                        .await?,
                    ))
                }
                StateBackendType::Sqlite => {
                    let sqlite_config = config
                        .sqlite
                        .expect("SQLite JSON backend config is required");
                    Ok(StateBackendFactories::Sqlite(
                        SqliteStateOperatorBackendFactory::new(
                            sqlite_config.database_path,
                            sqlite_config.max_connections,
                            sqlite_config.state_table_name,
                        )
                        .await?,
                    ))
                }
            }
        };

        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(init_future))
    }
}

pub trait StateOperatorBackendFactory {
    fn create<V>(&self, namespace: &str) -> Arc<dyn StateOperatorBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static;
}

impl StateOperatorBackendFactory for StateBackendFactories {
    fn create<V>(&self, namespace: &str) -> Arc<dyn StateOperatorBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static,
    {
        match self {
            StateBackendFactories::InMemory(factory) => factory.create(namespace),
            StateBackendFactories::Postgres(factory) => factory.create(namespace),
            StateBackendFactories::Sqlite(factory) => factory.create(namespace),
        }
    }
}
