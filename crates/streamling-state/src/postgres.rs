/// State Backend backed by Postgres. Uses JSONB for storing state values.
///
/// It uses the following table schema:
///
/// ```sql
/// CREATE TABLE streamling.state (
///   namespace TEXT,
///   key TEXT,
///   data JSONB NOT NULL,
///   created_at TIMESTAMPTZ DEFAULT NOW(),
///   PRIMARY KEY(namespace, key)
/// );
/// ```
/// Namespace can be used to separate different applications or versions.
/// Key is used to identify the state value (e.g. individual operator).
/// Data is the actual state value stored in JSONB format.
use crate::{
    StateBackendError, StateBackendErrorKind, StateKey, StateOperatorBackend,
    StateOperatorBackendFactory,
};
use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolOptions;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Row};
use std::fmt::Debug;
use std::sync::Arc;
use tracing::info;

const DEFAULT_MAX_CONNECTIONS: u32 = 20;
const DEFAULT_SCHEMA_NAME: &str = "streamling";
const DEFAULT_TABLE_NAME: &str = "state";

/// Ceiling on waiting for a pool connection (including establishing one).
/// sqlx's default is 30s, which is LONGER than the engine's entire shutdown
/// budget at the fleet-default grace (30s grace → 20s budget): with the
/// default, an unreachable backend during the terminal offset commit could
/// never surface its error before the watchdog force-exited — observed in
/// the field as a silent drain ending in "shutdown budget exceeded" with
/// only a sqlx pool WARN as a clue. 5s is orders of magnitude above a
/// healthy acquire (sub-ms once warm) and small enough that every caller's
/// error arm — checkpoint persistence, terminal commit, ClickHouse split
/// state — reports within even the smallest (5s) budget's neighborhood.
const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const IDENTIFIER_PATTERN: &str = r"^[A-Za-z_][A-Za-z0-9_]*$";

pub struct PostgresStateOperatorBackendFactory {
    pool: Arc<PgPool>,
    state_schema_name: String,
    state_table_name: String,
}

impl PostgresStateOperatorBackendFactory {
    pub async fn new(
        connection_url: String,
        max_connections: Option<u32>,
        state_schema_name: Option<String>,
        state_table_name: Option<String>,
        acquire_timeout: Option<std::time::Duration>,
    ) -> Result<Self, StateBackendError> {
        let state_schema_name =
            state_schema_name.unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());
        let state_table_name = state_table_name.unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());

        Self::validate_identifier(&state_schema_name)
            .map_err(|e| panic!("Invalid schema name: {}", e))
            .unwrap();

        Self::validate_identifier(&state_table_name)
            .map_err(|e| panic!("Invalid table name: {}", e))
            .unwrap();

        let pool_options: PoolOptions<Postgres> = PoolOptions::default()
            .max_connections(max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
            .min_connections(1)
            .acquire_timeout(acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT))
            .test_before_acquire(true);

        let pool = pool_options
            .connect(connection_url.as_str())
            .await
            .map_err(|e| {
                StateBackendError::with_source(
                    StateBackendErrorKind::Connection,
                    "failed to connect to Postgres",
                    e,
                )
            })?;
        let pool = Arc::new(pool);

        Self::initialize(
            pool.clone(),
            state_schema_name.as_str(),
            state_table_name.as_str(),
        )
        .await?;

        Ok(Self {
            pool,
            state_schema_name,
            state_table_name,
        })
    }

    fn validate_identifier(id: &str) -> Result<(), String> {
        let re = Regex::new(IDENTIFIER_PATTERN).unwrap();
        if !re.is_match(id) {
            return Err(format!(
                "Invalid identifier '{}'. Must match {}",
                id, IDENTIFIER_PATTERN
            ));
        }
        Ok(())
    }

    pub async fn initialize(
        pool: Arc<PgPool>,
        state_schema_name: &str,
        state_table_name: &str,
    ) -> Result<(), StateBackendError> {
        // Postgres's `CREATE ... IF NOT EXISTS` is not atomic against
        // concurrent creation: two state backends initializing the same
        // schema/table (e.g. several stateful operators of one pipeline
        // starting together) can both pass the existence check, and the loser
        // gets a duplicate-object error (42P06/42P07/42710) or a unique
        // violation on a catalog index (23505). Either way the object exists,
        // which is this function's postcondition — treat it as success.
        fn creation_race_lost(e: &sqlx::Error) -> bool {
            e.as_database_error()
                .and_then(|db| db.code())
                .is_some_and(|code| matches!(code.as_ref(), "23505" | "42P06" | "42P07" | "42710"))
        }

        match sqlx::query(
            format!(
                r#"
                CREATE SCHEMA IF NOT EXISTS {};
            "#,
                state_schema_name
            )
            .as_str(),
        )
        .execute(pool.as_ref())
        .await
        {
            Ok(_) => {}
            Err(e) if creation_race_lost(&e) => {}
            Err(e) => {
                return Err(StateBackendError::with_source(
                    StateBackendErrorKind::Initialization,
                    "failed to create schema",
                    e,
                ));
            }
        }

        match sqlx::query(
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {}.{} (
                    namespace TEXT,
                    key TEXT,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY(namespace, key)
                );
            "#,
                state_schema_name, state_table_name
            )
            .as_str(),
        )
        .execute(pool.as_ref())
        .await
        {
            Ok(_) => Ok(()),
            Err(e) if creation_race_lost(&e) => Ok(()),
            Err(e) => Err(StateBackendError::with_source(
                StateBackendErrorKind::Initialization,
                "failed to create state table",
                e,
            )),
        }
    }
}

impl StateOperatorBackendFactory for PostgresStateOperatorBackendFactory {
    fn create<V>(&self, namespace: &str) -> Arc<dyn StateOperatorBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Debug + 'static,
    {
        let full_state_table_name = format!("{}.{}", self.state_schema_name, self.state_table_name);
        Arc::new(PostgresStateOperatorBackend::new(
            self.pool.clone(),
            full_state_table_name,
            namespace,
        ))
    }
}

#[derive(Debug)]
struct PostgresStateOperatorBackend {
    pool: Arc<PgPool>,
    full_state_table_name: String,
    namespace: String,
}

impl PostgresStateOperatorBackend {
    fn new(pool: Arc<PgPool>, full_state_table_name: String, namespace: &str) -> Self {
        info!(
            "Creating a new Postgres JSON state backend for namespace: '{}' (table: {})",
            namespace, full_state_table_name
        );

        Self {
            pool,
            full_state_table_name,
            namespace: namespace.to_string(),
        }
    }
}

#[async_trait]
impl<V> StateOperatorBackend<V> for PostgresStateOperatorBackend
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Debug + 'static,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError> {
        let result = sqlx::query(
            format!(
                r#"
                SELECT data
                FROM {}
                WHERE namespace = $1 AND key = $2
            "#,
                self.full_state_table_name
            )
            .as_str(),
        )
        .bind(self.namespace.clone())
        .bind(key.0)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            StateBackendError::with_source(StateBackendErrorKind::Query, "failed to fetch state", e)
        })?;

        if result.is_none() {
            return Ok(None);
        }

        let data = result.unwrap();
        let data: Json<V> = data.try_get(0).map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Query,
                "failed to read data column",
                e,
            )
        })?;

        Ok(Some(data.0))
    }

    async fn put(&self, key: StateKey, value: V) -> Result<(), StateBackendError> {
        sqlx::query(
            format!(
                r#"
                INSERT INTO {} ( namespace, key, data, created_at )
                VALUES ( $1, $2, $3, NOW() )
                ON CONFLICT (namespace, key) DO UPDATE
                SET data = EXCLUDED.data
            "#,
                self.full_state_table_name
            )
            .as_str(),
        )
        .bind(self.namespace.clone())
        .bind(key.0)
        .bind(Json(value))
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

    /// One statement for the whole batch (UNNEST + upsert). The default
    /// trait impl's sequential puts cost one network round trip per entry,
    /// which dominated terminal-commit latency for high-partition-count
    /// Kafka sources (~0.18s/partition observed).
    async fn put_many(&self, entries: Vec<(StateKey, V)>) -> Result<(), StateBackendError>
    where
        V: Send + 'static,
    {
        if entries.is_empty() {
            return Ok(());
        }
        let mut keys: Vec<String> = Vec::with_capacity(entries.len());
        let mut values: Vec<Json<serde_json::Value>> = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            keys.push(key.0);
            values.push(Json(serde_json::to_value(&value).map_err(|e| {
                StateBackendError::with_source(
                    StateBackendErrorKind::Query,
                    "failed to serialize state value",
                    e,
                )
            })?));
        }
        sqlx::query(
            format!(
                r#"
                INSERT INTO {} ( namespace, key, data, created_at )
                SELECT $1, k, d, NOW()
                FROM UNNEST($2::text[], $3::jsonb[]) AS t(k, d)
                ON CONFLICT (namespace, key) DO UPDATE
                SET data = EXCLUDED.data
            "#,
                self.full_state_table_name
            )
            .as_str(),
        )
        .bind(self.namespace.clone())
        .bind(keys)
        .bind(values)
        .execute(self.pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(
                StateBackendErrorKind::Query,
                "failed to batch-update state",
                e,
            )
        })
    }

    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError> {
        sqlx::query(
            format!(
                r#"
                DELETE FROM {}
                WHERE namespace = $1 AND key = $2
            "#,
                self.full_state_table_name
            )
            .as_str(),
        )
        .bind(self.namespace.clone())
        .bind(key.0)
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
                WHERE namespace = $1
            "#,
                self.full_state_table_name
            )
            .as_str(),
        )
        .bind(self.namespace.clone())
        .execute(self.pool.as_ref())
        .await
        .map(|_| ())
        .map_err(|e| {
            StateBackendError::with_source(StateBackendErrorKind::Query, "failed to clear state", e)
        })
    }
}
