use crate::dynamic_table::{DynamicTableBackend, DynamicTableBackendError, extract_string_values};
use crate::error::Result as StreamlingResult;
use crate::error::ResultExt;
use crate::retry::retry_forever_with_backoff_async_returning;
use crate::streamling_user_err;
use async_trait::async_trait;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use futures::future::join_all;
use regex::Regex;
use sqlx::pool::PoolOptions;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Executor, PgPool, Postgres};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use streamling_config::app_config::PostgresDynamicTableBackendConfig;
use tokio::sync::{OnceCell, RwLock};
use tracing::{debug, error, info, trace};

const DEFAULT_MAX_CONNECTIONS: u32 = 20;
const DEFAULT_SCHEMA_NAME: &str = "streamling";
const IDENTIFIER_PATTERN: &str = r"^[A-Za-z_][A-Za-z0-9_]*$";
const CACHE_WRITE_LOCK_QUERY: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
const CACHE_LOAD_CURSOR_NAME: &str = "streamling_dynamic_table_cache";
const CACHE_LOAD_PAGE_SIZE: usize = 1_000;
/// Statement timeout for each individual database query (30 seconds)
const STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// PostgreSQL factory that maintains the connection pool and schema information
pub struct PostgresDynamicTableBackendFactory {
    pool: Arc<OnceCell<Arc<PgPool>>>,
    config: PostgresDynamicTableBackendConfig,
    dt_schema_name: String,
}

impl PostgresDynamicTableBackendFactory {
    pub fn new(
        config: PostgresDynamicTableBackendConfig,
    ) -> Result<Self, DynamicTableBackendError> {
        let dt_schema_name = config
            .dt_schema_name
            .clone()
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

        Self::validate_identifier(&dt_schema_name).map_err(|e| {
            DynamicTableBackendError::Initialization(format!("Invalid schema name: {}", e))
        })?;

        Ok(Self {
            pool: Arc::new(OnceCell::new()),
            config,
            dt_schema_name,
        })
    }

    fn validate_identifier(id: &str) -> StreamlingResult<()> {
        let re = Regex::new(IDENTIFIER_PATTERN).unwrap();
        if !re.is_match(id) {
            return Err(streamling_user_err!(
                "Invalid identifier '{}'. Must match {}",
                id,
                IDENTIFIER_PATTERN
            ));
        }
        Ok(())
    }

    async fn initialize_schema(
        pool: Arc<PgPool>,
        dt_schema_name: &str,
    ) -> Result<(), DynamicTableBackendError> {
        trace!("Initializing schema: {}", dt_schema_name);
        let result = sqlx::query(
            format!(
                r#"
                CREATE SCHEMA IF NOT EXISTS "{}";
            "#,
                dt_schema_name
            )
            .as_str(),
        )
        .execute(pool.as_ref())
        .await;

        match result {
            Ok(_) => {
                trace!("Schema {} initialized successfully", dt_schema_name);
                Ok(())
            }
            Err(e) => {
                let err = DynamicTableBackendError::Initialization(format!(
                    "Failed to initialize schema '{}': {}",
                    dt_schema_name, e
                ));
                error!("{}", err);
                Err(err)
            }
        }
    }

    pub async fn ensure_table_exists(
        pool: Arc<PgPool>,
        full_table_name: String,
        column_name: &str,
    ) -> Result<bool, DynamicTableBackendError> {
        // Try to query the column to check if table and column exist
        // We only check the value column since time_column has a default value
        let check_query = format!(
            r#"SELECT "{}" FROM {} LIMIT 0"#,
            column_name, full_table_name
        );
        trace!(
            "Checking table existence: {} with column: {}",
            full_table_name, column_name
        );
        let result = sqlx::query(&check_query).execute(pool.as_ref()).await;

        match result {
            Ok(_) => {
                // Table exists and has the column
                trace!(
                    "Table {} exists with column {}",
                    full_table_name, column_name
                );
                Ok(true)
            }
            Err(e) => {
                let error_msg = e.to_string().to_lowercase();
                // Check if error is about missing column (table exists but column doesn't)
                // PostgreSQL error: "column \"column_name\" does not exist"
                if error_msg.contains("column") && error_msg.contains("does not exist") {
                    let err = DynamicTableBackendError::Initialization(format!(
                        "Table {} may already exist but does not have the expected column '{}'. Please ensure the table exists and has the expected column.",
                        full_table_name, column_name
                    ));
                    error!("{}", err);
                    Err(err)
                } else if error_msg.contains("relation") && error_msg.contains("does not exist")
                    || error_msg.contains("schema") && error_msg.contains("does not exist")
                {
                    // If error is about missing table or schema (relation/schema does not exist), return false
                    // This means we need to create both schema and table
                    trace!(
                        "Table or schema does not exist for {}: {}",
                        full_table_name, e
                    );
                    Ok(false)
                } else {
                    // Other errors (permissions, etc.) return error
                    let err = DynamicTableBackendError::Initialization(format!(
                        "Dynamic table postgres error: Failed to check table existence for {}: {}",
                        full_table_name, e
                    ));
                    error!("{}", err);
                    Err(err)
                }
            }
        }
    }

    pub async fn create_backend(
        &self,
        backend_entity_name: String,
        schema: Option<String>,
        column: Option<String>,
        time_column: Option<String>,
        max_batch_size: usize,
    ) -> Result<PostgresDynamicTableBackend, DynamicTableBackendError> {
        debug!(
            "Creating dynamic table backend: entity={}, schema={:?}, column={:?}, time_column={:?}",
            backend_entity_name, schema, column, time_column
        );

        Self::validate_identifier(backend_entity_name.as_str()).map_err(|e| {
            let err =
                DynamicTableBackendError::Initialization(format!("Invalid table name: {}", e));
            error!("{}", err);
            err
        })?;

        // Use provided schema or fall back to factory's dt_schema_name
        let schema_name = schema.unwrap_or_else(|| self.dt_schema_name.clone());
        Self::validate_identifier(&schema_name).map_err(|e| {
            let err =
                DynamicTableBackendError::Initialization(format!("Invalid schema name: {}", e));
            error!("{}", err);
            err
        })?;

        // Use provided column or default to "value"
        let column_name = column.unwrap_or_else(|| "value".to_string());
        Self::validate_identifier(&column_name).map_err(|e| {
            let err =
                DynamicTableBackendError::Initialization(format!("Invalid column name: {}", e));
            error!("{}", err);
            err
        })?;

        if let Some(time_column_name) = &time_column {
            Self::validate_identifier(time_column_name).map_err(|e| {
                let err = DynamicTableBackendError::Initialization(format!(
                    "Invalid time column name: {}",
                    e
                ));
                error!("{}", err);
                err
            })?;
        }

        let full_table_name = format!("{}.{}", schema_name, backend_entity_name);
        trace!("Full table name: {}", full_table_name);

        // Return backend that will initialize lazily
        Ok(PostgresDynamicTableBackend::new(
            self.pool.clone(),
            self.config.clone(),
            full_table_name,
            schema_name,
            column_name,
            time_column,
            max_batch_size,
        ))
    }
}

#[derive(Debug)]
struct PostgresDynamicTableCache {
    updated_at: Option<String>,
    values: HashSet<Box<str>>,
}

impl PostgresDynamicTableCache {
    fn append(&mut self, update: Self) {
        self.updated_at = update.updated_at;
        self.values.extend(update.values);
    }
}

/// A lightweight PostgreSQL backend instance that shares the connection pool
#[derive(Debug)]
pub struct PostgresDynamicTableBackend {
    pool: Arc<OnceCell<Arc<PgPool>>>,
    config: PostgresDynamicTableBackendConfig,
    full_table_name: String,
    dt_schema_name: String,
    column_name: String,
    time_column_name: String,
    cache: Option<RwLock<Option<PostgresDynamicTableCache>>>,
    max_batch_size: usize,
}

impl PostgresDynamicTableBackend {
    fn new(
        pool: Arc<OnceCell<Arc<PgPool>>>,
        config: PostgresDynamicTableBackendConfig,
        full_table_name: String,
        dt_schema_name: String,
        column_name: String,
        time_column_name: Option<String>,
        max_batch_size: usize,
    ) -> Self {
        let cache_enabled = config.cache_enabled && time_column_name.is_some();
        debug!(
            table = %full_table_name,
            cache_enabled,
            time_column = ?time_column_name.as_deref(),
            "Creating PostgreSQL dynamic table backend"
        );

        let cache = cache_enabled.then(|| RwLock::new(None));
        let time_column_name = time_column_name.unwrap_or_else(|| "updated_at".to_string());

        Self {
            pool,
            config,
            full_table_name,
            dt_schema_name,
            column_name,
            time_column_name,
            cache,
            max_batch_size,
        }
    }

    async fn latest_update(&self, pool: Arc<PgPool>) -> Option<String> {
        let query: Arc<str> = format!(
            r#"SELECT MAX("{}")::TEXT FROM {}"#,
            self.time_column_name, self.full_table_name
        )
        .into();
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let operation_name = format!("DynamicTable latest_update ({})", self.full_table_name);

        retry_forever_with_backoff_async_returning(
            || {
                let pool = pool.clone();
                let query = query.clone();
                let full_table_name = full_table_name.clone();
                async move {
                    sqlx::query_scalar::<_, Option<String>>(query.as_ref())
                        .fetch_one(pool.as_ref())
                        .await
                        .streamling_with_context(|| {
                            format!("failed to read latest update from table {full_table_name}")
                        })
                }
            },
            &operation_name,
        )
        .await
    }

    async fn load_cache(
        &self,
        pool: Arc<PgPool>,
        updated_since: Option<&str>,
    ) -> (PostgresDynamicTableCache, usize) {
        // Serialized append-only writers assign CLOCK_TIMESTAMP after taking the
        // table lock; use a dedicated version cursor if that contract changes.
        let max_query: Arc<str> = format!(
            r#"SELECT MAX("{}")::TEXT FROM {}"#,
            self.time_column_name, self.full_table_name
        )
        .into();
        let declare_cursor_query: Arc<str> = if updated_since.is_some() {
            format!(
                r#"
                DECLARE {CACHE_LOAD_CURSOR_NAME} NO SCROLL CURSOR FOR
                SELECT "{}"::TEXT
                FROM {}
                WHERE "{}" IS NOT NULL AND "{}" > $1::TIMESTAMPTZ
                "#,
                self.column_name, self.full_table_name, self.column_name, self.time_column_name
            )
        } else {
            format!(
                r#"
                DECLARE {CACHE_LOAD_CURSOR_NAME} NO SCROLL CURSOR FOR
                SELECT "{}"::TEXT
                FROM {}
                WHERE "{}" IS NOT NULL
                "#,
                self.column_name, self.full_table_name, self.column_name
            )
        }
        .into();
        let fetch_page_query: Arc<str> =
            format!("FETCH FORWARD {CACHE_LOAD_PAGE_SIZE} FROM {CACHE_LOAD_CURSOR_NAME}").into();
        let updated_since = updated_since.map(str::to_owned);
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let operation_name = format!("DynamicTable load_cache ({})", self.full_table_name);

        retry_forever_with_backoff_async_returning(
            || {
                let pool = pool.clone();
                let max_query = max_query.clone();
                let declare_cursor_query = declare_cursor_query.clone();
                let fetch_page_query = fetch_page_query.clone();
                let updated_since = updated_since.clone();
                let full_table_name = full_table_name.clone();
                async move {
                    let mut transaction = pool.begin().await.streamling_with_context(|| {
                        format!("failed to begin cache load for table {full_table_name}")
                    })?;
                    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                        .execute(&mut *transaction)
                        .await
                        .streamling_with_context(|| {
                            format!("failed to configure cache load for table {full_table_name}")
                        })?;

                    let updated_at = sqlx::query_scalar::<_, Option<String>>(max_query.as_ref())
                        .fetch_one(&mut *transaction)
                        .await
                        .streamling_with_context(|| {
                            format!("failed to read cache version from table {full_table_name}")
                        })?;

                    let cursor_query = sqlx::query(declare_cursor_query.as_ref());
                    let cursor_query = if let Some(updated_since) = updated_since {
                        cursor_query.bind(updated_since)
                    } else {
                        cursor_query
                    };
                    cursor_query
                        .execute(&mut *transaction)
                        .await
                        .streamling_with_context(|| {
                            format!("failed to open cache cursor for table {full_table_name}")
                        })?;

                    let mut values = HashSet::new();
                    let mut pages_loaded = 0;
                    loop {
                        let page: Vec<String> = sqlx::query_scalar(fetch_page_query.as_ref())
                            .fetch_all(&mut *transaction)
                            .await
                            .streamling_with_context(|| {
                                format!("failed to fetch cache page from table {full_table_name}")
                            })?;
                        if page.is_empty() {
                            break;
                        }
                        pages_loaded += 1;
                        values.extend(page.into_iter().map(String::into_boxed_str));
                    }

                    transaction.commit().await.streamling_with_context(|| {
                        format!("failed to finish cache load for table {full_table_name}")
                    })?;
                    Ok((
                        PostgresDynamicTableCache { updated_at, values },
                        pages_loaded,
                    ))
                }
            },
            &operation_name,
        )
        .await
    }

    async fn refresh_cache(&self, pool: Arc<PgPool>) {
        let cache = self
            .cache
            .as_ref()
            .expect("cache is present when refresh_cache is called");
        let updated_at = self.latest_update(pool.clone()).await;

        if let Some(cached) = cache.read().await.as_ref()
            && cached.updated_at == updated_at
        {
            return;
        }

        let mut cached = cache.write().await;
        if let Some(current) = cached.as_ref()
            && current.updated_at == updated_at
        {
            return;
        }

        let updated_since = cached
            .as_ref()
            .and_then(|current| current.updated_at.as_deref())
            .map(str::to_owned);
        let load_started_at = Instant::now();
        let (refreshed, pages_loaded) = self.load_cache(pool, updated_since.as_deref()).await;
        let elapsed_ms = load_started_at.elapsed().as_millis();

        if let Some(current) = cached.as_mut() {
            let added_entries = refreshed.values.len();
            current.append(refreshed);
            debug!(
                table = %self.full_table_name,
                added_entries,
                total_entries = current.values.len(),
                pages_loaded,
                elapsed_ms = ?elapsed_ms,
                previous_watermark = ?updated_since.as_deref(),
                watermark = ?current.updated_at.as_deref(),
                "Refreshed PostgreSQL dynamic table cache"
            );
        } else {
            info!(
                table = %self.full_table_name,
                total_entries = refreshed.values.len(),
                pages_loaded,
                elapsed_ms = ?elapsed_ms,
                watermark = ?refreshed.updated_at.as_deref(),
                "Populated PostgreSQL dynamic table cache"
            );
            *cached = Some(refreshed);
        }
    }

    fn build_contains_result(
        &self,
        string_array: &StringArray,
        existing_set: &HashSet<Box<str>>,
    ) -> ArrayRef {
        let mut builder = BooleanBuilder::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let value = string_array.value(i);
                let contains_value = existing_set.contains(value);
                builder.append_value(contains_value);
                trace!(
                    "[contains] for table name '{}' with value '{}' result '{:?}'",
                    self.full_table_name, value, contains_value
                );
            }
        }
        Arc::new(builder.finish())
    }

    async fn validate_cache_time_column(
        &self,
        pool: &PgPool,
    ) -> Result<(), DynamicTableBackendError> {
        let query = format!(
            r#"SELECT MAX("{}")::TEXT FROM {} WHERE "{}" >= CURRENT_TIMESTAMP AND FALSE"#,
            self.time_column_name, self.full_table_name, self.time_column_name
        );

        sqlx::query(&query).execute(pool).await.map_err(|e| {
            let err = DynamicTableBackendError::Initialization(format!(
                "Failed to validate cache time column '{}' for table {}: {}",
                self.time_column_name, self.full_table_name, e
            ));
            error!("{}", err);
            err
        })?;

        Ok(())
    }

    /// Check if a batch of values exist in the table (internal method that doesn't split batches)
    /// Retries forever with exponential backoff. Statement timeout prevents individual queries from hanging.
    /// Uses Arc to wrap values so retry clones are cheap (reference count increment only).
    async fn contains_batch(
        &self,
        pool: Arc<PgPool>,
        value_indices: Vec<(usize, String)>,
    ) -> HashSet<Box<str>> {
        if value_indices.is_empty() {
            return HashSet::new();
        }

        // Wrap in Arc once before the retry loop to avoid cloning Vec on each retry
        let value_indices: Arc<[(usize, String)]> = Arc::from(value_indices.into_boxed_slice());
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let column_name: Arc<str> = Arc::from(self.column_name.as_str());
        let operation_name = format!("DynamicTable contains_batch ({})", self.full_table_name);

        retry_forever_with_backoff_async_returning(
            || {
                // Arc clones are cheap (just incrementing reference counts)
                let pool = pool.clone();
                let value_indices = value_indices.clone();
                let full_table_name = full_table_name.clone();
                let column_name = column_name.clone();
                async move {
                    // Create batch query using ANY operator
                    let placeholders: Vec<String> = (1..=value_indices.len())
                        .map(|i| format!("${}", i))
                        .collect();
                    let query = format!(
                        r#"
                        SELECT "{}"
                        FROM {}
                        WHERE "{}" = ANY(ARRAY[{}])
                        "#,
                        column_name,
                        full_table_name,
                        column_name,
                        placeholders.join(", ")
                    );

                    let mut sqlx_query = sqlx::query_scalar::<_, String>(&query);
                    for (_, value) in value_indices.iter() {
                        sqlx_query = sqlx_query.bind(value);
                    }

                    let existing_values: Vec<String> = sqlx_query
                        .fetch_all(pool.as_ref())
                        .await
                        .streamling_with_context(|| {
                            format!(
                                "failed to check if values exist in table {}",
                                full_table_name
                            )
                        })?;

                    Ok(existing_values
                        .into_iter()
                        .map(String::into_boxed_str)
                        .collect())
                }
            },
            &operation_name,
        )
        .await
    }

    /// Append a batch of values to the table (internal method that doesn't split batches)
    /// Retries forever with exponential backoff. Statement timeout prevents individual queries from hanging.
    /// Uses Arc to wrap values so retry clones are cheap (reference count increment only).
    async fn append_batch(&self, pool: Arc<PgPool>, values: Vec<String>) {
        if values.is_empty() {
            return;
        }

        let values_len = values.len();
        // Wrap in Arc once before the retry loop to avoid cloning Vec on each retry
        let values: Arc<[String]> = Arc::from(values.into_boxed_slice());
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let column_name: Arc<str> = Arc::from(self.column_name.as_str());
        let time_column_name: Arc<str> = Arc::from(self.time_column_name.as_str());
        let serialize_write = self.cache.is_some();
        let operation_name = format!("DynamicTable append_batch ({})", self.full_table_name);

        retry_forever_with_backoff_async_returning(
            || {
                // Arc clones are cheap (just incrementing reference counts)
                let pool = pool.clone();
                let values = values.clone();
                let full_table_name = full_table_name.clone();
                let column_name = column_name.clone();
                let time_column_name = time_column_name.clone();
                async move {
                    // Create batch insert query with UNNEST for multiple values
                    let placeholders: Vec<String> =
                        (1..=values.len()).map(|i| format!("${}", i)).collect();
                    let query = if serialize_write {
                        format!(
                            r#"
                            INSERT INTO {} ("{}", "{}")
                            SELECT new_value, CLOCK_TIMESTAMP()
                            FROM (
                                SELECT DISTINCT unnest(ARRAY[{}]) AS new_value
                            ) AS new_values
                            ON CONFLICT ("{}") DO NOTHING
                            "#,
                            full_table_name,
                            column_name,
                            time_column_name,
                            placeholders.join(", "),
                            column_name
                        )
                    } else {
                        format!(
                            r#"
                            INSERT INTO {} ("{}")
                            SELECT DISTINCT unnest(ARRAY[{}])
                            ON CONFLICT ("{}") DO NOTHING
                            "#,
                            full_table_name,
                            column_name,
                            placeholders.join(", "),
                            column_name
                        )
                    };

                    let mut sqlx_query = sqlx::query(&query);
                    for value in values.iter() {
                        sqlx_query = sqlx_query.bind(value);
                    }

                    if serialize_write {
                        let mut transaction = pool.begin().await.streamling_with_context(|| {
                            format!("failed to begin cached write to table {full_table_name}")
                        })?;

                        // Cached writes serialize per table; add a dedicated version row
                        // only if write throughput makes this lock material.
                        sqlx::query(CACHE_WRITE_LOCK_QUERY)
                            .bind(full_table_name.as_ref())
                            .execute(&mut *transaction)
                            .await
                            .streamling_with_context(|| {
                                format!(
                                    "failed to serialize cached writes to table {full_table_name}"
                                )
                            })?;

                        sqlx_query
                            .execute(&mut *transaction)
                            .await
                            .streamling_with_context(|| {
                                format!("failed to append values to table {full_table_name}")
                            })?;
                        transaction.commit().await.streamling_with_context(|| {
                            format!("failed to commit cached write to table {full_table_name}")
                        })?;
                    } else {
                        sqlx_query
                            .execute(pool.as_ref())
                            .await
                            .streamling_with_context(|| {
                                format!("failed to append values to table {full_table_name}")
                            })?;
                    }

                    Ok(())
                }
            },
            &operation_name,
        )
        .await;

        trace!(
            "[append_batch] for table name '{}' with {} values",
            self.full_table_name, values_len
        );
    }

    /// Ensure the pool is initialized and table exists. Called lazily on first use.
    async fn get_pool(&self) -> Result<Arc<PgPool>, DynamicTableBackendError> {
        debug!(
            "Initializing PostgreSQL connection pool for dynamic table: {}",
            self.full_table_name
        );
        self.pool
            .get_or_try_init(|| async {
                // Initialize pool
                trace!(
                    "Connecting to PostgreSQL for table: {}",
                    self.full_table_name
                );

                let connect_options = PgConnectOptions::from_str(
                    self.config.connection_url().as_str(),
                )
                .map_err(|e| {
                    let err = DynamicTableBackendError::Connection(format!(
                        "Failed to parse connection URL for table {}: {}",
                        self.full_table_name, e
                    ));
                    error!("{}", err);
                    err
                })?;

                // Set statement_timeout via SQL after connect (compatible with Neon and other pooled providers)
                let statement_timeout_ms = STATEMENT_TIMEOUT.as_millis();
                let pool_options: PoolOptions<Postgres> = PoolOptions::default()
                    .max_connections(
                        self.config
                            .max_connections
                            .unwrap_or(DEFAULT_MAX_CONNECTIONS),
                    )
                    .after_connect(move |conn: &mut sqlx::PgConnection, _meta| {
                        Box::pin(async move {
                            conn.execute(
                                format!("SET statement_timeout = {}", statement_timeout_ms)
                                    .as_str(),
                            )
                            .await?;
                            Ok(())
                        })
                    });

                let pool = pool_options
                    .connect_with(connect_options)
                    .await
                    .map_err(|e| {
                        let err = DynamicTableBackendError::Connection(format!(
                            "Failed to connect to Postgres for table {}: {}",
                            self.full_table_name, e
                        ));
                        error!("{}", err);
                        err
                    })?;

                trace!(
                    "Successfully connected to PostgreSQL for table: {}",
                    self.full_table_name
                );
                let pool_arc = Arc::new(pool);

                // Check if table exists
                trace!("Checking if table exists: {}", self.full_table_name);
                let table_exists = PostgresDynamicTableBackendFactory::ensure_table_exists(
                    pool_arc.clone(),
                    self.full_table_name.clone(),
                    &self.column_name,
                )
                .await
                .map_err(|e| {
                    let err = DynamicTableBackendError::Initialization(format!(
                        "Failed to check table existence for {}: {}",
                        self.full_table_name, e
                    ));
                    error!("{}", err);
                    err
                })?;

                // Create table if it doesn't exist
                if !table_exists {
                    // Initialize schema only if table doesn't exist
                    // This requires less permissions if user prefers to create the table themselves
                    debug!(
                        "Table does not exist, initializing schema '{}' for table: {}",
                        self.dt_schema_name, self.full_table_name
                    );
                    PostgresDynamicTableBackendFactory::initialize_schema(
                        pool_arc.clone(),
                        &self.dt_schema_name,
                    )
                    .await
                    .map_err(|e| {
                        let err = DynamicTableBackendError::Initialization(format!(
                            "Failed to initialize schema '{}' for table {}: {}",
                            self.dt_schema_name, self.full_table_name, e
                        ));
                        error!("{}", err);
                        err
                    })?;

                    info!(
                        "Creating PostgreSQL dynamic table: {}",
                        self.full_table_name
                    );
                    let create_table_sql = format!(
                        r#"
                            CREATE TABLE IF NOT EXISTS {} (
                                "{}" TEXT PRIMARY KEY,
                                "{}" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP()
                            );
                        "#,
                        self.full_table_name, self.column_name, self.time_column_name
                    );
                    let bare_table = self
                        .full_table_name
                        .rsplit_once('.')
                        .map(|(_, table)| table)
                        .unwrap_or(self.full_table_name.as_str());
                    let index_name =
                        build_time_column_index_name(bare_table, &self.time_column_name);
                    let create_index_sql = format!(
                        r#"CREATE INDEX IF NOT EXISTS "{}" ON {} ("{}")"#,
                        index_name, self.full_table_name, self.time_column_name
                    );

                    let mut transaction = pool_arc.begin().await.map_err(|e| {
                        let err = DynamicTableBackendError::Initialization(format!(
                            "Failed to begin transaction for table {}: {}",
                            self.full_table_name, e
                        ));
                        error!("{}", err);
                        err
                    })?;
                    sqlx::query(&create_table_sql)
                        .execute(&mut *transaction)
                        .await
                        .map_err(|e| {
                            let err = DynamicTableBackendError::Initialization(format!(
                                "Failed to create table {}: {}",
                                self.full_table_name, e
                            ));
                            error!("{}", err);
                            err
                        })?;
                    sqlx::query(&create_index_sql)
                        .execute(&mut *transaction)
                        .await
                        .map_err(|e| {
                            let err = DynamicTableBackendError::Initialization(format!(
                                "Failed to create index {} on table {}: {}",
                                index_name, self.full_table_name, e
                            ));
                            error!("{}", err);
                            err
                        })?;
                    transaction.commit().await.map_err(|e| {
                        let err = DynamicTableBackendError::Initialization(format!(
                            "Failed to commit transaction for table {}: {}",
                            self.full_table_name, e
                        ));
                        error!("{}", err);
                        err
                    })?;
                    info!("Successfully created table: {}", self.full_table_name);
                } else {
                    debug!(
                        "Table {} already exists, skipping creation",
                        self.full_table_name
                    );
                }

                if self.cache.is_some() {
                    self.validate_cache_time_column(pool_arc.as_ref()).await?;
                }

                Ok::<Arc<PgPool>, DynamicTableBackendError>(pool_arc)
            })
            .await
            .cloned()
    }
}

/// Build the deterministic index name for a dynamic table's time column.
///
/// PostgreSQL silently truncates identifiers to `NAMEDATALEN - 1` (63) bytes,
/// so a naive `idx_{table}_{column}` can collide after truncation — two long
/// names can truncate to the same identifier, and `CREATE INDEX IF NOT EXISTS`
/// would then no-op against the wrong index on subsequent startups. When the
/// name exceeds the limit we keep a readable prefix and append a short, stable
/// hash of the full name, keeping it unique and under 63 bytes. Identifiers are
/// ASCII (`[A-Za-z0-9_]`), so byte slicing is always on a char boundary.
fn build_time_column_index_name(bare_table: &str, time_column: &str) -> String {
    const MAX_IDENT_BYTES: usize = 63;
    let raw = format!("idx_{}_{}", bare_table, time_column);
    if raw.len() <= MAX_IDENT_BYTES {
        return raw;
    }
    let hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        raw.hash(&mut hasher);
        format!("{:08x}", hasher.finish())
    };
    // '_' + 8 hex hash chars.
    let prefix_len = MAX_IDENT_BYTES - 1 - hash.len();
    format!("{}_{}", &raw[..prefix_len], hash)
}

/// Remove duplicate values from `(index, value)` pairs, keeping only the first
/// occurrence of each value. This shrinks the SQL parameter list and may reduce
/// batch count in `contains()` without changing results, because
/// `build_contains_result` looks up every original row value in the returned
/// set — not in `value_indices`.
fn deduplicate_value_indices(value_indices: Vec<(usize, String)>) -> Vec<(usize, String)> {
    let mut seen: HashSet<String> = HashSet::new();
    value_indices
        .into_iter()
        .filter(|(_, value)| seen.insert(value.clone()))
        .collect()
}

#[async_trait]
impl DynamicTableBackend for PostgresDynamicTableBackend {
    async fn append(&self, values: ArrayRef) -> Result<(), DynamicTableBackendError> {
        trace!("append() called for table: {}", self.full_table_name);
        let pool = self.get_pool().await.map_err(|e| {
            error!(
                "Failed to get pool for table {} in append(): {}",
                self.full_table_name, e
            );
            e
        })?;

        let values = extract_string_values(values)?;
        let values_len = values.len();
        if values_len == 0 {
            return Ok(());
        }

        // Split into batches if exceeding max_batch_size
        if values_len <= self.max_batch_size {
            // Single batch - process directly
            self.append_batch(pool, values).await;
        } else {
            // Split into multiple batches and process concurrently
            let chunks: Vec<Vec<String>> = values
                .chunks(self.max_batch_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let chunks_len = chunks.len();
            trace!(
                "Splitting {} values into {} concurrent batches for table {}",
                values_len, chunks_len, self.full_table_name
            );

            // Process all chunks concurrently
            let futures: Vec<_> = chunks
                .into_iter()
                .map(|chunk| {
                    let pool = pool.clone();
                    async move { self.append_batch(pool, chunk).await }
                })
                .collect();

            // Wait for all batches to complete
            join_all(futures).await;
        }

        trace!(
            "[append] completed for table name '{}' with {} values",
            self.full_table_name, values_len
        );

        Ok(())
    }

    async fn contains(&self, values: ArrayRef) -> Result<ArrayRef, DynamicTableBackendError> {
        trace!("contains() called for table: {}", self.full_table_name);
        let pool = self.get_pool().await.map_err(|e| {
            error!(
                "Failed to get pool for table {} in contains(): {}",
                self.full_table_name, e
            );
            e
        })?;

        // Cast ArrayRef to StringArray
        let string_array = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DynamicTableBackendError::StringArrayExpected)?;

        if let Some(cache) = &self.cache {
            // Check for table changes on every invocation, including empty/all-null batches.
            self.refresh_cache(pool).await;
            let cached = cache.read().await;
            let existing_set = &cached
                .as_ref()
                .expect("cache is loaded after refresh_cache")
                .values;
            return Ok(self.build_contains_result(string_array, existing_set));
        }

        // Uncached queries need owned strings for retries.
        let value_indices: Vec<(usize, String)> = (0..string_array.len())
            .filter_map(|i| {
                if !string_array.is_null(i) {
                    Some((i, string_array.value(i).to_string()))
                } else {
                    None
                }
            })
            .collect();

        if value_indices.is_empty() {
            return Ok(self.build_contains_result(string_array, &HashSet::new()));
        }

        // Deduplicate values so each unique value is queried only once.
        let value_indices = deduplicate_value_indices(value_indices);

        let existing_set = if value_indices.len() <= self.max_batch_size {
            // Single batch - process directly
            self.contains_batch(pool, value_indices).await
        } else {
            // Split into multiple batches and process concurrently
            let chunks: Vec<Vec<(usize, String)>> = value_indices
                .chunks(self.max_batch_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            trace!(
                "Splitting {} values into {} concurrent batches for contains() on table {}",
                chunks.iter().map(|c| c.len()).sum::<usize>(),
                chunks.len(),
                self.full_table_name
            );

            // Process all chunks concurrently
            let futures: Vec<_> = chunks
                .into_iter()
                .map(|chunk| {
                    let pool = pool.clone();
                    async move { self.contains_batch(pool, chunk).await }
                })
                .collect();

            // Wait for all batches to complete and combine results
            let results = join_all(futures).await;
            let mut combined_set = HashSet::new();
            for chunk_set in results {
                combined_set.extend(chunk_set);
            }
            combined_set
        };

        Ok(self.build_contains_result(string_array, &existing_set))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn postgres_config(cache_enabled: bool) -> PostgresDynamicTableBackendConfig {
        PostgresDynamicTableBackendConfig {
            host: "localhost".to_string(),
            port: 5432,
            db: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            sslmode: "disable".to_string(),
            max_connections: None,
            dt_schema_name: None,
            cache_enabled,
        }
    }

    #[test]
    fn cache_append_preserves_existing_values() {
        let mut cache = PostgresDynamicTableCache {
            updated_at: Some("old".to_string()),
            values: HashSet::from([Box::<str>::from("existing")]),
        };
        let update = PostgresDynamicTableCache {
            updated_at: Some("new".to_string()),
            values: HashSet::from([Box::<str>::from("appended")]),
        };

        cache.append(update);

        assert_eq!(cache.updated_at.as_deref(), Some("new"));
        assert_eq!(cache.values.len(), 2);
        assert!(cache.values.contains("existing"));
        assert!(cache.values.contains("appended"));
    }

    #[tokio::test]
    async fn cache_requires_flag_and_explicit_time_column() {
        let disabled_factory = PostgresDynamicTableBackendFactory::new(postgres_config(false))
            .expect("factory should be valid");

        let disabled = disabled_factory
            .create_backend(
                "disabled".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
            )
            .await
            .expect("backend should be valid");
        assert!(disabled.cache.is_none());

        let enabled_factory = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let missing_time_column = enabled_factory
            .create_backend("missing_time_column".to_string(), None, None, None, 1000)
            .await
            .expect("backend should be valid");
        assert!(missing_time_column.cache.is_none());
        assert_eq!(missing_time_column.time_column_name, "updated_at");

        let cached = enabled_factory
            .create_backend(
                "cached".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
            )
            .await
            .expect("backend should be valid");
        assert!(cached.cache.is_some());
    }
    #[test]
    fn build_time_column_index_name_stays_under_limit() {
        // Short names pass through unchanged.
        let short = build_time_column_index_name("blocks", "block_timestamp");
        assert_eq!(short, "idx_blocks_block_timestamp");
        assert!(short.len() <= 63);

        // Long names are truncated + hash-suffixed to stay under 63 bytes, and
        // stay deterministic across calls so IF NOT EXISTS is stable at startup.
        let long_bare_table = "a".repeat(60);
        let long_col = "updated_at";
        let name = build_time_column_index_name(&long_bare_table, long_col);
        assert!(name.len() <= 63);
        assert_eq!(
            name,
            build_time_column_index_name(&long_bare_table, long_col)
        );

        // Distinct long names produce distinct identifiers (no silent truncation collision).
        let other = build_time_column_index_name(&"b".repeat(60), long_col);
        assert_ne!(name, other);

        // The readable `idx_` prefix is preserved for debuggability.
        assert!(name.starts_with("idx_"));
    }

    #[test]
    fn deduplicate_value_indices_removes_duplicates() {
        let input = vec![
            (0, "a".to_string()),
            (1, "b".to_string()),
            (2, "a".to_string()), // duplicate of 0
            (3, "c".to_string()),
            (4, "b".to_string()), // duplicate of 1
            (5, "a".to_string()), // duplicate of 0
        ];

        let result = deduplicate_value_indices(input);

        // Only first occurrence of each value survives.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (0, "a".to_string()));
        assert_eq!(result[1], (1, "b".to_string()));
        assert_eq!(result[2], (3, "c".to_string()));
    }

    #[test]
    fn deduplicate_value_indices_preserves_all_unique() {
        let input: Vec<(usize, String)> = (0..5).map(|i| (i, format!("val{}", i))).collect();

        let result = deduplicate_value_indices(input);

        assert_eq!(result.len(), 5);
    }

    #[test]
    fn deduplicate_value_indices_empty_input() {
        let result = deduplicate_value_indices(vec![]);
        assert!(result.is_empty());
    }
}
