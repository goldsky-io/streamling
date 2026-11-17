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
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Row};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

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

/// Ceiling on one state statement (read, upsert or delete), enforced on the
/// client. A healthy write takes well under a second, including the largest
/// plugin checkpoints seen (tens of MB of JSONB). The statement is retried
/// once on a fresh connection, so two bounds (120 s) still fit inside the
/// default 300 s checkpoint timeout and a dead connection surfaces as an error
/// for that checkpoint instead of hanging until the kernel gives up on the
/// socket (observed: 31 minutes).
const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(60);

const IDENTIFIER_PATTERN: &str = r"^[A-Za-z_][A-Za-z0-9_]*$";

pub struct PostgresStateOperatorBackendFactory {
    pool: Arc<PgPool>,
    state_schema_name: String,
    state_table_name: String,
    statement_timeout: Duration,
}

impl PostgresStateOperatorBackendFactory {
    pub async fn new(
        connection_url: String,
        max_connections: Option<u32>,
        state_schema_name: Option<String>,
        state_table_name: Option<String>,
        acquire_timeout: Option<std::time::Duration>,
        statement_timeout: Option<Duration>,
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
            statement_timeout: statement_timeout.unwrap_or(DEFAULT_STATEMENT_TIMEOUT),
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
            self.statement_timeout,
        ))
    }
}

#[derive(Debug)]
struct PostgresStateOperatorBackend {
    pool: Arc<PgPool>,
    full_state_table_name: String,
    namespace: String,
    statement_timeout: Duration,
}

impl PostgresStateOperatorBackend {
    fn new(
        pool: Arc<PgPool>,
        full_state_table_name: String,
        namespace: &str,
        statement_timeout: Duration,
    ) -> Self {
        info!(
            "Creating a new Postgres JSON state backend for namespace: '{}' (table: {})",
            namespace, full_state_table_name
        );

        Self {
            pool,
            full_state_table_name,
            namespace: namespace.to_string(),
            statement_timeout,
        }
    }

    /// Runs one state statement under `statement_timeout`, retrying it once
    /// on a fresh connection if the bound elapses.
    ///
    /// Without the bound, a statement on a connection whose peer died
    /// silently waits on the kernel's TCP retransmission timeout: a checkpoint
    /// write was observed hanging 31 minutes before failing with EOF, with no
    /// log in between. On timeout the connection is detached and dropped
    /// rather than returned to the pool, so the retry (and every later
    /// acquire) cannot get the same dead socket back; see `execute_bounded`
    /// in streamling-core for the same pattern on the Postgres sink.
    ///
    /// Every statement here is an upsert, a delete or a read, so running one
    /// a second time after the first may have committed is harmless. Only a
    /// timeout is retried; a query error is returned as before.
    async fn run<'q>(
        &self,
        what: &'static str,
        fetch_row: bool,
        build: impl Fn() -> Query<'q, Postgres, PgArguments>,
    ) -> Result<Option<PgRow>, StateBackendError> {
        const ATTEMPTS: u32 = 2;
        for attempt in 1..=ATTEMPTS {
            let mut conn = self.pool.acquire().await.map_err(|e| {
                StateBackendError::with_source(StateBackendErrorKind::Connection, what, e)
            })?;
            let query = build();
            let statement = async {
                if fetch_row {
                    query.fetch_optional(&mut *conn).await
                } else {
                    query.execute(&mut *conn).await.map(|_| None)
                }
            };
            match tokio::time::timeout(self.statement_timeout, statement).await {
                Ok(result) => {
                    return result.map_err(|e| {
                        StateBackendError::with_source(StateBackendErrorKind::Query, what, e)
                    });
                }
                Err(_elapsed) => {
                    drop(conn.detach());
                    warn!(
                        "state backend: statement for namespace '{}' did not complete within {:?} (attempt {}/{}); discarded the connection",
                        self.namespace, self.statement_timeout, attempt, ATTEMPTS
                    );
                }
            }
        }
        Err(StateBackendError::new(
            StateBackendErrorKind::Connection,
            format!(
                "{what}: statement did not complete within {:?} on {ATTEMPTS} connections",
                self.statement_timeout
            ),
        ))
    }
}

#[async_trait]
impl<V> StateOperatorBackend<V> for PostgresStateOperatorBackend
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Debug + 'static,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError> {
        let sql = format!(
            r#"
                SELECT data
                FROM {}
                WHERE namespace = $1 AND key = $2
            "#,
            self.full_state_table_name
        );
        let result = self
            .run("failed to fetch state", true, || {
                sqlx::query(&sql).bind(&self.namespace).bind(&key.0)
            })
            .await?;

        let Some(data) = result else {
            return Ok(None);
        };

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
        let sql = format!(
            r#"
                INSERT INTO {} ( namespace, key, data, created_at )
                VALUES ( $1, $2, $3, NOW() )
                ON CONFLICT (namespace, key) DO UPDATE
                SET data = EXCLUDED.data
            "#,
            self.full_state_table_name
        );
        self.run("failed to update state", false, || {
            sqlx::query(&sql)
                .bind(&self.namespace)
                .bind(&key.0)
                .bind(Json(&value))
        })
        .await
        .map(|_| ())
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
        let sql = format!(
            r#"
                INSERT INTO {} ( namespace, key, data, created_at )
                SELECT $1, k, d, NOW()
                FROM UNNEST($2::text[], $3::jsonb[]) AS t(k, d)
                ON CONFLICT (namespace, key) DO UPDATE
                SET data = EXCLUDED.data
            "#,
            self.full_state_table_name
        );
        self.run("failed to batch-update state", false, || {
            sqlx::query(&sql)
                .bind(&self.namespace)
                .bind(&keys)
                .bind(&values)
        })
        .await
        .map(|_| ())
    }

    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError> {
        let sql = format!(
            r#"
                DELETE FROM {}
                WHERE namespace = $1 AND key = $2
            "#,
            self.full_state_table_name
        );
        self.run("failed to remove state", false, || {
            sqlx::query(&sql).bind(&self.namespace).bind(&key.0)
        })
        .await
        .map(|_| ())
    }

    async fn clear(&self) -> Result<(), StateBackendError> {
        let sql = format!(
            r#"
                DELETE FROM {}
                WHERE namespace = $1
            "#,
            self.full_state_table_name
        );
        self.run("failed to clear state", false, || {
            sqlx::query(&sql).bind(&self.namespace)
        })
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Minimal fake PostgreSQL server (same as streamling-core's `pg.rs`
    /// test helper): completes the startup handshake, then never responds to
    /// anything else while still reading the client's bytes. This models a
    /// peer that died silently mid-connection.
    async fn spawn_silent_postgres() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();

        // Sanctioned: test-only fake server; dies with the test runtime.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                accepted.fetch_add(1, Ordering::SeqCst);

                #[allow(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    // Read the client's StartupMessage (length-prefixed, no type byte).
                    let mut len_buf = [0u8; 4];
                    if socket.read_exact(&mut len_buf).await.is_err() {
                        return;
                    }
                    let len = (u32::from_be_bytes(len_buf) as usize).saturating_sub(4);
                    let mut body = vec![0u8; len];
                    if socket.read_exact(&mut body).await.is_err() {
                        return;
                    }

                    let mut reply: Vec<u8> = Vec::new();
                    // AuthenticationOk
                    reply.extend([b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
                    // ParameterStatus messages the client parses during connect
                    for (key, value) in [
                        ("server_version", "14.0"),
                        ("client_encoding", "UTF8"),
                        ("DateStyle", "ISO, MDY"),
                    ] {
                        let payload_len = 4 + key.len() + 1 + value.len() + 1;
                        reply.push(b'S');
                        reply.extend((payload_len as u32).to_be_bytes());
                        reply.extend(key.as_bytes());
                        reply.push(0);
                        reply.extend(value.as_bytes());
                        reply.push(0);
                    }
                    // BackendKeyData
                    reply.extend([b'K', 0, 0, 0, 12]);
                    reply.extend(1234u32.to_be_bytes());
                    reply.extend(5678u32.to_be_bytes());
                    // ReadyForQuery (idle)
                    reply.extend([b'Z', 0, 0, 0, 5, b'I']);
                    if socket.write_all(&reply).await.is_err() {
                        return;
                    }

                    // Handshake complete. Play dead: keep draining client bytes
                    // so writes succeed, but never send another byte back.
                    let mut sink_buf = [0u8; 4096];
                    while socket
                        .read(&mut sink_buf)
                        .await
                        .map(|n| n > 0)
                        .unwrap_or(false)
                    {}
                });
            }
        });

        (addr, connections)
    }

    fn backend_against(addr: SocketAddr, bound: Duration) -> PostgresStateOperatorBackend {
        let url = format!(
            "postgres://user@{}:{}/db?sslmode=disable",
            addr.ip(),
            addr.port()
        );
        let pool = PoolOptions::<Postgres>::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(&url)
            .expect("lazy pool");
        PostgresStateOperatorBackend::new(
            Arc::new(pool),
            "streamling.state".to_string(),
            "test_ns",
            bound,
        )
    }

    /// Reproduces the 31-minute hang: a state write against a peer that
    /// stopped responding must fail within two statement bounds, on two
    /// distinct physical connections (the dead one is discarded, not
    /// reused), and the error must name the timeout. Repeating the call
    /// catches a permit leak: a plain drop instead of a detach would exhaust
    /// the 2-connection pool on the second call.
    #[tokio::test]
    async fn a_state_write_to_a_silent_peer_times_out_on_fresh_connections() {
        let bound = Duration::from_millis(300);
        let (addr, connections) = spawn_silent_postgres().await;
        let backend = backend_against(addr, bound);

        for call in 1..=2usize {
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                StateOperatorBackend::<serde_json::Value>::put(
                    &backend,
                    StateKey::from("k"),
                    serde_json::json!({"v": 1}),
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("call {call} must not hang on a silent server"));

            let err = result.expect_err("must time out against a silent server");
            assert!(
                format!("{err:?}").contains("did not complete within"),
                "call {call} must fail via the statement bound, got: {err:?}"
            );
            assert!(
                started.elapsed() >= bound * 2,
                "call {call} must retry once before failing: {:?}",
                started.elapsed()
            );
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "call {call} took too long: {:?}",
                started.elapsed()
            );
            assert_eq!(
                connections.load(Ordering::SeqCst),
                call * 2,
                "each attempt must open a fresh physical connection"
            );
        }
    }

    /// Reads are bounded too: a restore must not hang on a dead socket either.
    #[tokio::test]
    async fn a_state_read_from_a_silent_peer_times_out() {
        let bound = Duration::from_millis(300);
        let (addr, _connections) = spawn_silent_postgres().await;
        let backend = backend_against(addr, bound);

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            StateOperatorBackend::<serde_json::Value>::get(&backend, StateKey::from("k")),
        )
        .await
        .expect("must not hang on a silent server");

        let err = result.expect_err("must time out against a silent server");
        assert!(
            format!("{err:?}").contains("did not complete within"),
            "got: {err:?}"
        );
    }
}
