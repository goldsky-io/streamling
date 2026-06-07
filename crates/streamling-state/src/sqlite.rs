/// State Backend backed by Sqlite.
///
/// It uses the following table schema:
///
/// ```sql
/// CREATE TABLE state (
///   namespace TEXT,
///   key TEXT,
///   data TEXT NOT NULL,
///   created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
///   PRIMARY KEY(namespace, key)
/// );
/// ```
/// Namespace can be used to separate different applications or versions.
/// Key is used to identify the state value (e.g. individual operator).
/// Data is the actual state value stored in JSON format.
use crate::{
    StateBackendError, StateBackendErrorKind, StateKey, StateOperatorBackend,
    StateOperatorBackendFactory,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Row, SqlitePool};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_TABLE_NAME: &str = "state";

pub struct SqliteStateOperatorBackendFactory {
    pool: Arc<SqlitePool>,
    state_table_name: String,
}

impl SqliteStateOperatorBackendFactory {
    pub async fn new(
        database_path: String,
        max_connections: Option<u32>,
        state_table_name: Option<String>,
    ) -> Result<Self, StateBackendError> {
        let state_table_name = state_table_name.unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());

        let options = SqliteConnectOptions::from_str(format!("sqlite:{}", database_path).as_str())
            .unwrap()
            .create_if_missing(true);

        let pool = PoolOptions::<sqlx::Sqlite>::new()
            .max_connections(max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
            .connect_with(options)
            .await
            .map_err(|e| {
                StateBackendError::with_source(
                    StateBackendErrorKind::Connection,
                    "failed to create SQLite connection pool",
                    e,
                )
            })?;

        let pool = Arc::new(pool);

        Self::initialize(pool.clone(), &state_table_name).await?;

        Ok(Self {
            pool,
            state_table_name,
        })
    }

    async fn initialize(
        pool: Arc<SqlitePool>,
        state_table_name: &str,
    ) -> Result<(), StateBackendError> {
        sqlx::query(
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {} (
                    namespace TEXT,
                    key TEXT,
                    data TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(namespace, key)
                );
            "#,
                state_table_name
            )
            .as_str(),
        )
        .execute(pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Initialization,
                "failed to create state table",
                e,
            )
        })
    }
}

impl StateOperatorBackendFactory for SqliteStateOperatorBackendFactory {
    fn create<V>(&self, namespace: &str) -> Arc<dyn StateOperatorBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static,
    {
        Arc::new(SqliteStateOperatorBackend::new(
            self.pool.clone(),
            self.state_table_name.clone(),
            namespace,
        ))
    }
}

#[derive(Debug)]
struct SqliteStateOperatorBackend {
    pool: Arc<SqlitePool>,
    state_table_name: String,
    namespace: String,
}

impl SqliteStateOperatorBackend {
    fn new(pool: Arc<SqlitePool>, state_table_name: String, namespace: &str) -> Self {
        info!(
            "Creating a new SQLite JSON state backend for namespace: {}",
            namespace
        );

        Self {
            pool,
            state_table_name,
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl<V> StateOperatorBackend<V> for SqliteStateOperatorBackend
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Debug + 'static,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError> {
        let result = sqlx::query(
            format!(
                r#"
                SELECT data
                FROM {}
                WHERE namespace = ? AND key = ?
            "#,
                self.state_table_name
            )
            .as_str(),
        )
        .bind(&self.namespace)
        .bind(&key.0)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            StateBackendError::with_source(StateBackendErrorKind::Query, "failed to fetch state", e)
        })?;

        if result.is_none() {
            return Ok(None);
        }

        let data = result.unwrap();
        let json_str: String = data.try_get(0).map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Query,
                "failed to read data column",
                e,
            )
        })?;

        serde_json::from_str(&json_str).map(Some).map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Serialization,
                "failed to deserialize state",
                e,
            )
        })
    }

    async fn put(&self, key: StateKey, value: V) -> Result<(), StateBackendError> {
        let json_str = serde_json::to_string(&value).unwrap();
        sqlx::query(
            format!(
                r#"
                INSERT INTO {} (namespace, key, data, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(namespace, key) DO UPDATE SET data = excluded.data
            "#,
                self.state_table_name
            )
            .as_str(),
        )
        .bind(&self.namespace)
        .bind(&key.0)
        .bind(&json_str)
        .execute(self.pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Query,
                "failed to update state",
                e,
            )
        })
    }

    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError> {
        sqlx::query(
            format!(
                r#"
                DELETE FROM {}
                WHERE namespace = ? AND key = ?
            "#,
                self.state_table_name
            )
            .as_str(),
        )
        .bind(&self.namespace)
        .bind(&key.0)
        .execute(self.pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Query,
                "failed to remove state",
                e,
            )
        })
    }

    async fn clear(&self) -> Result<(), StateBackendError> {
        sqlx::query(
            format!(
                r#"
                DELETE FROM {}
                WHERE namespace = ?
            "#,
                self.state_table_name
            )
            .as_str(),
        )
        .bind(&self.namespace)
        .execute(self.pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(StateBackendErrorKind::Query, "failed to clear state", e)
        })
    }
}
