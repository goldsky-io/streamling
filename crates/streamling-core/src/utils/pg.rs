use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt, StreamlingError};
use crate::types::{i256::I256Type, u256::U256Type};
use crate::utils::parse_primary_key_columns;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use sqlx::Executor;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use streamling_config::{PostgresSinkConfig, postgres_connection_url};
use tracing::{debug, error, info, warn};

/// Manages PostgreSQL connection pool
#[derive(Clone, Debug)]
pub struct PostgresConnection {
    pool: Arc<PgPool>,
}

impl PostgresConnection {
    /// Create a new PostgreSQL connection pool from config
    pub async fn new(config: &PostgresSinkConfig) -> Result<Self> {
        Self::new_with_parallelism(config, 1).await
    }

    /// Create a new PostgreSQL connection pool sized for the given parallelism level
    pub async fn new_with_parallelism(
        config: &PostgresSinkConfig,
        parallelism: usize,
    ) -> Result<Self> {
        let port: u16 = config.port.parse().map_err(|_| {
            StreamlingError::user(format!("invalid Postgres port '{}'", config.port))
        })?;

        let url = postgres_connection_url(
            config.host.clone(),
            port,
            config.db.clone(),
            config.user.clone(),
            config.pass.clone(),
            config.sslmode.clone(),
        );

        // Scale pool size with parallelism: at least 10, or parallelism + headroom
        let max_connections = 10u32.max(parallelism as u32 + 2);

        debug!(
            "Connecting to PostgreSQL: {}@{}:{}/{} (max_connections={}, parallelism={})",
            config.user, config.host, port, config.db, max_connections, parallelism
        );

        let connect_options = PgConnectOptions::from_str(&url).map_err(|e| {
            StreamlingError::user_with_cause("invalid PostgreSQL connection URL", e)
        })?;

        // Set statement_timeout via SQL after connect (compatible with Neon and other pooled providers)
        let statement_timeout_ms = Duration::from_secs(config.statement_timeout_secs).as_millis();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(config.pool_acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.pool_idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.pool_max_lifetime_secs))
            .test_before_acquire(true)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    conn.execute(
                        format!("SET statement_timeout = {}", statement_timeout_ms).as_str(),
                    )
                    .await?;
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await
            .map_err(|e| {
                StreamlingError::retriable_with_cause("failed to connect to PostgreSQL", e)
            })?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// Get a reference to the pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Execute a query on a pooled connection with an optional client-side time
/// bound, returning the number of rows affected.
///
/// The server-side `statement_timeout` cannot fire when the connection is dead
/// (e.g. a NAT/firewall silently dropped the flow after the statement was
/// sent): the client just awaits a response that will never arrive, and sqlx
/// exposes no TCP keepalive to detect it. Without this bound such an await
/// hangs the caller forever with no logs. `None` (from
/// `statement_timeout_secs = 0`) disables the bound — matching the server-side
/// "no timeout" contract.
///
/// On timeout the connection is detached from the pool and dropped rather
/// than returned: the next acquire would otherwise receive the same dead
/// socket, and sqlx's return-to-pool cleanup can itself block on it. Dropping
/// a detached connection closes the socket without any await, and the pool
/// opens a fresh replacement on the next acquire.
///
/// Delivery semantics: like every retried error on these paths (e.g. a
/// connection drop after the server committed but before the response
/// arrived), a timeout can lead to re-executing a statement that already
/// committed — at-least-once. Upsert/delete statements absorb this; callers
/// issuing plain INSERTs (no conflict target) can observe duplicates, which is
/// their pre-existing retry contract, not a property of this bound.
pub async fn execute_bounded(
    pool: &PgPool,
    query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    timeout: Option<Duration>,
) -> Result<u64> {
    let Some(timeout) = timeout else {
        let result = query
            .execute(pool)
            .await
            .streamling_context("failed to execute statement")?;
        return Ok(result.rows_affected());
    };

    let mut conn = pool.acquire().await.map_err(|e| {
        StreamlingError::retriable_with_cause("failed to acquire PostgreSQL connection", e)
    })?;

    match tokio::time::timeout(timeout, query.execute(&mut *conn)).await {
        Ok(result) => {
            let result = result.streamling_context("failed to execute statement")?;
            Ok(result.rows_affected())
        }
        Err(_elapsed) => {
            drop(conn.detach());
            Err(StreamlingError::retriable(format!(
                "statement did not complete within {timeout:?}; discarded the connection and will retry on a fresh one"
            )))
        }
    }
}

/// PostgreSQL type information for an Arrow field
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresTypeInfo {
    /// PostgreSQL column type for CREATE TABLE (e.g., "NUMERIC(20,0)")
    pub column_type: String,
    /// If Some, bind as String and use this SQL cast expression (e.g., "numeric(20,0)").
    /// If None, bind as native Rust type.
    pub string_cast_sql: Option<String>,
}

/// Get PostgreSQL type information for an Arrow field
/// This is the single source of truth for Arrow → PostgreSQL type mapping
pub fn get_postgres_type_info(field: &Field) -> PostgresTypeInfo {
    // Check for U256/I256 types that become NUMERIC(78,0)
    if matches!(field.data_type(), DataType::FixedSizeBinary(32))
        && (U256Type::is_u256_metadata(field.metadata())
            || I256Type::is_i256_metadata(field.metadata()))
    {
        return PostgresTypeInfo {
            column_type: "NUMERIC(78,0)".to_string(),
            string_cast_sql: Some("numeric(78,0)".to_string()),
        };
    }

    match field.data_type() {
        // UInt64 that becomes NUMERIC(20,0) - bind as string with cast
        DataType::UInt64 => PostgresTypeInfo {
            column_type: "NUMERIC(20,0)".to_string(),
            string_cast_sql: Some("numeric(20,0)".to_string()),
        },
        // Decimal128 that becomes NUMERIC(precision, scale) - bind as string with cast
        DataType::Decimal128(precision, scale) => PostgresTypeInfo {
            column_type: format!("NUMERIC({}, {})", precision, scale),
            string_cast_sql: Some(format!("numeric({},{})", precision, scale)),
        },
        // Decimal256 that becomes NUMERIC(precision, scale) - bind as string with cast
        DataType::Decimal256(precision, scale) => PostgresTypeInfo {
            column_type: format!("NUMERIC({}, {})", precision, scale),
            string_cast_sql: Some(format!("numeric({},{})", precision, scale)),
        },
        // Nested types that become JSONB - bind as string with cast
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => PostgresTypeInfo {
            column_type: "JSONB".to_string(),
            string_cast_sql: Some("jsonb".to_string()),
        },
        // Integer types - bind as native types
        DataType::Int8 => PostgresTypeInfo {
            column_type: "SMALLINT".to_string(),
            string_cast_sql: None,
        },
        DataType::Int16 => PostgresTypeInfo {
            column_type: "SMALLINT".to_string(),
            string_cast_sql: None,
        },
        DataType::Int32 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::Int64 => PostgresTypeInfo {
            column_type: "BIGINT".to_string(),
            string_cast_sql: None,
        },
        // Unsigned integer types (except UInt64) - bind as native types
        DataType::UInt8 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::UInt16 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::UInt32 => PostgresTypeInfo {
            column_type: "BIGINT".to_string(),
            string_cast_sql: None,
        },
        // Boolean - bind as native type
        DataType::Boolean => PostgresTypeInfo {
            column_type: "BOOLEAN".to_string(),
            string_cast_sql: None,
        },
        // Float types - bind as native types
        DataType::Float16 | DataType::Float32 => PostgresTypeInfo {
            column_type: "REAL".to_string(),
            string_cast_sql: None,
        },
        DataType::Float64 => PostgresTypeInfo {
            column_type: "DOUBLE PRECISION".to_string(),
            string_cast_sql: None,
        },
        // String types - bind as string, no cast needed
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PostgresTypeInfo {
            column_type: "TEXT".to_string(),
            string_cast_sql: None,
        },
        // Binary types - bind as Vec<u8>, no cast needed
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            PostgresTypeInfo {
                column_type: "BYTEA".to_string(),
                string_cast_sql: None,
            }
        }
        // Date types - bind as string with cast
        DataType::Date32 | DataType::Date64 => PostgresTypeInfo {
            column_type: "DATE".to_string(),
            string_cast_sql: Some("date".to_string()),
        },
        // Timestamp types - bind as string with cast
        DataType::Timestamp(_, _) => PostgresTypeInfo {
            column_type: "TIMESTAMP".to_string(),
            string_cast_sql: Some("timestamp".to_string()),
        },
        // Time types - bind as string with cast
        DataType::Time32(_) | DataType::Time64(_) => PostgresTypeInfo {
            column_type: "TIME".to_string(),
            string_cast_sql: Some("time".to_string()),
        },
        // Unknown types - default to TEXT, bind as string
        _ => {
            warn!(
                "Unmapped Arrow type {:?} for column '{}', defaulting to TEXT",
                field.data_type(),
                field.name()
            );
            PostgresTypeInfo {
                column_type: "TEXT".to_string(),
                string_cast_sql: None,
            }
        }
    }
}

/// Convert Arrow Field to PostgreSQL type string
pub fn arrow_field_to_postgres_type(field: &Field) -> String {
    get_postgres_type_info(field).column_type
}

/// Convert PostgreSQL type string to Arrow DataType
/// This is the inverse of `get_postgres_type_info` for type override scenarios
pub fn postgres_type_to_arrow_type(pg_type: &str) -> Result<DataType> {
    let pg_type_lower = pg_type.to_lowercase();
    let base_type = pg_type_lower
        .split('(')
        .next()
        .unwrap_or(&pg_type_lower)
        .trim();

    match base_type {
        "text" | "varchar" | "char" => Ok(DataType::Utf8),
        "smallint" => Ok(DataType::Int16),
        "integer" | "int" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "real" => Ok(DataType::Float32),
        "double precision" | "float" => Ok(DataType::Float64),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "bytea" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" | "timestamptz" => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        )),
        "jsonb" | "json" => Ok(DataType::Utf8),
        "numeric" | "decimal" => {
            if pg_type_lower.contains('(') {
                let params_start = pg_type_lower.find('(').unwrap();
                let params_end = pg_type_lower.rfind(')').ok_or_else(|| {
                    StreamlingError::user(format!(
                        "invalid PostgreSQL type '{}': missing closing parenthesis",
                        pg_type
                    ))
                })?;
                let params_str = &pg_type_lower[params_start + 1..params_end];
                let parts: Vec<&str> = params_str.split(',').collect();

                let precision = parts[0].trim().parse::<u8>().map_err(|_| {
                    StreamlingError::user(format!(
                        "invalid precision in PostgreSQL type '{}'",
                        pg_type
                    ))
                })?;

                let scale = if parts.len() > 1 {
                    parts[1].trim().parse::<i8>().map_err(|_| {
                        StreamlingError::user(format!(
                            "invalid scale in PostgreSQL type '{}'",
                            pg_type
                        ))
                    })?
                } else {
                    0
                };

                Ok(DataType::Decimal128(precision, scale))
            } else {
                // Default precision and scale for NUMERIC without parameters
                Ok(DataType::Decimal128(38, 9))
            }
        }
        _ => Err(StreamlingError::user(format!(
            "unsupported PostgreSQL type: {}",
            pg_type
        ))),
    }
}

/// Create schema and table if they don't exist
pub async fn create_schema_and_table_if_needed(
    pool: &PgPool,
    schema: &str,
    table: &str,
    source_schema: &SchemaRef,
    primary_key: Option<&String>,
    append_only_mode: bool,
    checkpoint_truncation: bool,
) -> Result<()> {
    // Create schema if needed
    let schema_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema);
    sqlx::query(&schema_sql).execute(pool).await.map_err(|e| {
        StreamlingError::retriable_with_cause(format!("failed to create schema '{}'", schema), e)
    })?;

    // Create table if needed
    let mut cols: Vec<String> = Vec::new();
    let mut has_gs_op_column = false;
    for f in source_schema.fields() {
        if f.name() == COLUMN_NAME_OP {
            has_gs_op_column = true;
            if !append_only_mode {
                // Omit _gs_op for normal sinks (only used for determining operation type)
                continue;
            }
        }
        let col_type = arrow_field_to_postgres_type(f);
        cols.push(format!(r#""{}" {}"#, f.name(), col_type));
    }

    // For append-only mode, ensure _gs_op column exists
    if append_only_mode && !has_gs_op_column {
        cols.push(format!(r#""{}" TEXT NOT NULL"#, COLUMN_NAME_OP));
    }

    // Add _gs_checkpoint_epoch column for checkpoint truncation
    if checkpoint_truncation {
        cols.push(r#""_gs_checkpoint_epoch" BIGINT NOT NULL DEFAULT 0"#.to_string());
    }

    let mut table_parts = vec![cols.join(", ")];
    if let Some(primary_key_str) = primary_key {
        let mut keys: Vec<String> = parse_primary_key_columns(primary_key_str)
            .iter()
            .map(|k| format!(r#""{}""#, k))
            .collect();

        // For append-only mode, add _gs_op to primary key for composite key
        if append_only_mode {
            keys.push(format!(r#""{}""#, COLUMN_NAME_OP));
        }

        if !keys.is_empty() {
            table_parts.push(format!("PRIMARY KEY ({})", keys.join(", ")));
        }
    }

    let create_table_sql = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"."{}" ({})"#,
        schema,
        table,
        table_parts.join(", ")
    );

    debug!("Creating PostgreSQL table: {}", create_table_sql);

    sqlx::query(&create_table_sql)
        .execute(pool)
        .await
        .map_err(|e| {
            StreamlingError::retriable_with_cause(
                format!("failed to create table '{}.{}'", schema, table),
                e,
            )
        })?;

    // Create index on _gs_checkpoint_epoch for efficient truncation
    if checkpoint_truncation {
        let index_name = format!("idx_{}_checkpoint_epoch", table);
        let create_index_sql = format!(
            r#"CREATE INDEX IF NOT EXISTS "{}" ON "{}"."{}" ("_gs_checkpoint_epoch")"#,
            index_name, schema, table
        );

        debug!("Creating checkpoint epoch index: {}", create_index_sql);

        sqlx::query(&create_index_sql)
            .execute(pool)
            .await
            .map_err(|e| {
                StreamlingError::retriable_with_cause(
                    format!(
                        "failed to create index '{}' on table '{}.{}'",
                        index_name, schema, table
                    ),
                    e,
                )
            })?;
    }

    Ok(())
}

/// Truncate finalized checkpoint data from the table
/// This is a best-effort operation - errors are logged but don't fail the pipeline
///
/// The DELETE runs inline in the sink's batch loop, so it carries the same
/// client-side bound as data statements (`execute_bounded`) — an unbounded
/// await here would wedge the sink exactly like a hung INSERT.
pub async fn truncate_finalized_checkpoint_data(
    pool: &PgPool,
    schema: &str,
    table: &str,
    finalized_epoch: u64,
    statement_timeout: Option<Duration>,
) -> Result<()> {
    let delete_sql = format!(
        r#"DELETE FROM "{}"."{}" WHERE "_gs_checkpoint_epoch" <= $1"#,
        schema, table
    );

    debug!(
        "Truncating landing table {}.{} for epochs <= {}",
        schema, table, finalized_epoch
    );

    let query = sqlx::query(&delete_sql).bind(finalized_epoch as i64);
    match execute_bounded(pool, query, statement_timeout).await {
        Ok(rows_deleted) => {
            info!(
                "Truncated {} rows from {}.{} for epoch {}",
                rows_deleted, schema, table, finalized_epoch
            );
            Ok(())
        }
        Err(e) => {
            // Log error but don't fail the pipeline - truncation is best-effort cleanup
            error!(
                "Failed to truncate landing table {}.{} for epoch {}: {}",
                schema, table, finalized_epoch, e
            );
            Ok(()) // Return Ok to not block the pipeline
        }
    }
}

/// Override schema for PostgreSQL inserts: convert U256/I256 and nested types to Utf8
pub fn override_schema_for_postgres_insert(source_schema: &SchemaRef) -> Schema {
    let fields: Vec<Field> = source_schema
        .fields()
        .iter()
        .map(|f| {
            if U256Type::is_u256_field(f) || I256Type::is_i256_field(f) {
                Field::new(f.name(), DataType::Utf8, f.is_nullable())
            } else if matches!(
                f.data_type(),
                DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _)
            ) {
                // Serialize nested types to JSON strings for insert (server casts to JSONB)
                Field::new(f.name(), DataType::Utf8, f.is_nullable())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_boolean_type() {
        let field = Field::new("flag", DataType::Boolean, false);
        assert_eq!(arrow_field_to_postgres_type(&field), "BOOLEAN");
    }

    #[test]
    fn test_int_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("i8", DataType::Int8, false)),
            "SMALLINT"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("i16", DataType::Int16, false)),
            "SMALLINT"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("i32", DataType::Int32, false)),
            "INTEGER"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("i64", DataType::Int64, false)),
            "BIGINT"
        );
    }

    #[test]
    fn test_uint_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("u8", DataType::UInt8, false)),
            "INTEGER"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("u16", DataType::UInt16, false)),
            "INTEGER"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("u32", DataType::UInt32, false)),
            "BIGINT"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("u64", DataType::UInt64, false)),
            "NUMERIC(20,0)"
        );
    }

    #[test]
    fn test_float_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("f32", DataType::Float32, false)),
            "REAL"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("f64", DataType::Float64, false)),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("text", DataType::Utf8, false)),
            "TEXT"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("large_text", DataType::LargeUtf8, false)),
            "TEXT"
        );
    }

    #[test]
    fn test_binary_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("bin", DataType::Binary, false)),
            "BYTEA"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("large_bin", DataType::LargeBinary, false)),
            "BYTEA"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new(
                "fixed_bin",
                DataType::FixedSizeBinary(32),
                false
            )),
            "BYTEA"
        );
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("dec128", DataType::Decimal128(10, 2), false)),
            "NUMERIC(10, 2)"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("dec256", DataType::Decimal256(20, 4), false)),
            "NUMERIC(20, 4)"
        );
    }

    #[test]
    fn test_date_types() {
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("date32", DataType::Date32, false)),
            "DATE"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new("date64", DataType::Date64, false)),
            "DATE"
        );
    }

    #[test]
    fn test_timestamp_type() {
        let field = Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, None),
            false,
        );
        assert_eq!(arrow_field_to_postgres_type(&field), "TIMESTAMP");
    }

    #[test]
    fn test_nested_types() {
        let empty_fields: Vec<Field> = vec![];
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new(
                "struct",
                DataType::Struct(empty_fields.into()),
                false
            )),
            "JSONB"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
                false
            )),
            "JSONB"
        );
        assert_eq!(
            arrow_field_to_postgres_type(&Field::new(
                "map",
                DataType::Map(Arc::new(Field::new("key", DataType::Utf8, false)), false),
                false
            )),
            "JSONB"
        );
    }

    #[test]
    fn test_u256_type() {
        let field = Field::new("u256", U256Type::new(), false).with_metadata(U256Type::metadata());
        assert_eq!(arrow_field_to_postgres_type(&field), "NUMERIC(78,0)");
    }

    #[test]
    fn test_i256_type() {
        let field = Field::new("i256", I256Type::new(), false).with_metadata(I256Type::metadata());
        assert_eq!(arrow_field_to_postgres_type(&field), "NUMERIC(78,0)");
    }

    #[test]
    fn test_unknown_type_defaults_to_text() {
        // Using a type that's not explicitly handled
        let field = Field::new(
            "unknown",
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
            false,
        );
        assert_eq!(arrow_field_to_postgres_type(&field), "TEXT");
    }

    use sqlx::postgres::PgPoolOptions;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Minimal fake PostgreSQL server: completes the startup handshake, then
    /// never responds to anything else while still reading (and ACKing) the
    /// client's bytes. This models a peer that died silently mid-connection —
    /// the client's write succeeds and its read waits forever, which is
    /// exactly the state that used to hang the sink with no logs.
    async fn spawn_silent_postgres() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                accepted.fetch_add(1, Ordering::SeqCst);

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

    /// Reproduces the silent-hang bug: without the client-side bound in
    /// `execute_bounded`, executing a statement against a peer that stopped
    /// responding awaits forever (no error, no retry, no log). With the bound
    /// it must fail retriable within the timeout, discard the dead connection
    /// (releasing its pool permit), and get a fresh physical connection on
    /// every subsequent attempt — the third attempt exists to catch a
    /// permit-leak regression (a plain drop instead of detach strands the
    /// permit in a background return-to-pool ping and exhausts a 2-connection
    /// pool by attempt 3).
    #[tokio::test]
    async fn test_bounded_execute_times_out_and_discards_dead_connection() {
        let bound = Duration::from_millis(300);
        let (addr, connections) = spawn_silent_postgres().await;
        let url = format!(
            "postgres://user@{}:{}/db?sslmode=disable",
            addr.ip(),
            addr.port()
        );
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(&url)
            .expect("lazy pool");

        for attempt in 1..=3u32 {
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                execute_bounded(&pool, sqlx::query("SELECT 1"), Some(bound)),
            )
            .await
            .unwrap_or_else(|_| panic!("attempt {attempt} must not hang on a silent server"));

            let err = result.expect_err("must time out against a silent server");
            assert!(err.is_retriable(), "timeout must be retriable: {err:?}");
            // Distinguish a genuine statement timeout from an acquire failure:
            // the bound must actually elapse and produce the timeout message.
            assert!(
                format!("{err:?}").contains("did not complete within"),
                "attempt {attempt} must fail via the statement bound, got: {err:?}"
            );
            assert!(
                started.elapsed() >= bound,
                "attempt {attempt} failed before the bound elapsed: {:?}",
                started.elapsed()
            );
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "attempt {attempt} took too long: {:?}",
                started.elapsed()
            );
            assert_eq!(
                connections.load(Ordering::SeqCst),
                attempt as usize,
                "each attempt must open a fresh physical connection (permit released by detach)"
            );
        }
    }
}
