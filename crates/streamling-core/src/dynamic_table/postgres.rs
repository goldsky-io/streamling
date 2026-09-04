use crate::dynamic_table::key_set::ArrowKeySet;
use crate::dynamic_table::{DynamicTableBackend, DynamicTableBackendError, extract_string_values};
use crate::error::Result as StreamlingResult;
use crate::error::ResultExt;
use crate::error::StreamlingError;
use crate::retry::{retry_forever_with_backoff_until_cancelled_returning, retry_if_retriable};
use crate::streamling_user_err;
use async_trait::async_trait;
use datafusion::arrow::array::builder::{BooleanBuilder, LargeStringBuilder};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, LargeStringArray, StringArray};
use futures::future::join_all;
use regex::Regex;
use sqlx::pool::PoolOptions;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Executor, PgPool, Postgres};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use streamling_config::app_config::PostgresDynamicTableBackendConfig;
use tokio::sync::{OnceCell, RwLock};
use tracing::{debug, error, info, trace, warn};

const DEFAULT_MAX_CONNECTIONS: u32 = 20;
const DEFAULT_SCHEMA_NAME: &str = "streamling";
const IDENTIFIER_PATTERN: &str = r"^[A-Za-z_][A-Za-z0-9_]*$";
const CACHE_WRITE_LOCK_QUERY: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
const CACHE_LOAD_CURSOR_NAME: &str = "streamling_dynamic_table_cache";
const CACHE_LOAD_PAGE_SIZE: usize = 1_000;
/// Statement timeout for each individual database query (30 seconds)
const STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// PostgreSQL error codes that indicate a transient infrastructure failure
/// rather than a permanent configuration problem: connection exceptions
/// (08000-08006), admin/cluster shutdown (57P01-57P03), and
/// too-many-connections (53300).
fn pg_code_is_transient(code: &str) -> bool {
    matches!(
        code,
        "08000" | "08001" | "08003" | "08004" | "08006" | "57P01" | "57P02" | "57P03" | "53300"
    )
}

/// True when a sqlx error is transient infrastructure trouble (lost
/// connection, pool exhaustion, server shutdown) that a retry can plausibly
/// recover from.
fn sqlx_error_is_transient(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
        sqlx::Error::Database(db) => db.code().is_some_and(|c| pg_code_is_transient(&c)),
        _ => false,
    }
}

/// Classify a sqlx error raised during lazy backend initialization:
/// transient failures become `Connection` (retried by `get_pool`), everything
/// else stays `Initialization` (permanent, fails fast).
fn classify_sqlx_error(operation: &str, table: &str, e: &sqlx::Error) -> DynamicTableBackendError {
    if sqlx_error_is_transient(e) {
        DynamicTableBackendError::Connection(format!("{operation} for table {table}: {e}"))
    } else {
        DynamicTableBackendError::Initialization(format!("{operation} for table {table}: {e}"))
    }
}

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
            // See `creation_race_lost`: two backends initializing the same
            // schema concurrently is normal; the loser's duplicate error means
            // the schema exists.
            Err(e) if crate::utils::pg::creation_race_lost(&e) => {
                trace!(
                    "Schema {} already exists (lost a concurrent creation race)",
                    dt_schema_name
                );
                Ok(())
            }
            Err(e) => {
                let err = classify_sqlx_error("Failed to initialize schema", dt_schema_name, &e);
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
                    // Other errors: transient infrastructure failures are
                    // retried by get_pool; permanent ones (permissions, etc.)
                    // fail fast.
                    let err = classify_sqlx_error(
                        "Failed to check table existence",
                        &full_table_name,
                        &e,
                    );
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
        cache_refresh_debounce_ms: u64,
        cache_enabled_override: Option<bool>,
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
            cache_refresh_debounce_ms,
            cache_enabled_override,
        ))
    }
}

#[derive(Debug)]
struct PostgresDynamicTableCache {
    updated_at: Option<String>,
    values: ArrowKeySet,
}

impl PostgresDynamicTableCache {
    fn append(
        &mut self,
        updated_at: Option<String>,
        keys: LargeStringArray,
    ) -> Result<(), DynamicTableBackendError> {
        self.values
            .extend_from(keys)
            .map_err(DynamicTableBackendError::Query)?;
        self.updated_at = updated_at;
        Ok(())
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
    /// Debounce window for the freshness check; 0 disables debouncing.
    pub(crate) cache_refresh_debounce_ms: u64,
    /// Monotonic base for the debounce clock.
    created_at: Instant,
    /// Millis since `created_at` at the last freshness check.
    last_freshness_check_ms: AtomicU64,
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
        cache_refresh_debounce_ms: u64,
        cache_enabled_override: Option<bool>,
    ) -> Self {
        let cache_enabled =
            cache_enabled_override.unwrap_or(config.cache_enabled) && time_column_name.is_some();
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
            cache_refresh_debounce_ms,
            created_at: Instant::now(),
            last_freshness_check_ms: AtomicU64::new(0),
        }
    }

    /// Retries with backoff until it succeeds or shutdown is requested (same drain
    /// rationale as `contains_batch`).
    async fn latest_update(
        &self,
        pool: Arc<PgPool>,
    ) -> Result<Option<String>, DynamicTableBackendError> {
        let query: Arc<str> = format!(
            r#"SELECT MAX("{}")::TEXT FROM {}"#,
            self.time_column_name, self.full_table_name
        )
        .into();
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let operation_name = format!("DynamicTable latest_update ({})", self.full_table_name);
        let mut shutdown = crate::shutdown::subscribe();

        retry_forever_with_backoff_until_cancelled_returning(
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
            &mut shutdown,
        )
        .await
        .ok_or_else(|| {
            DynamicTableBackendError::Query(format!("{} cancelled by shutdown", operation_name))
        })
    }

    /// Retries with backoff until it succeeds or shutdown is requested (same drain
    /// rationale as `contains_batch`).
    async fn load_cache(
        &self,
        pool: Arc<PgPool>,
        updated_since: Option<&str>,
    ) -> Result<(Option<String>, LargeStringArray, usize), DynamicTableBackendError> {
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
        let mut shutdown = crate::shutdown::subscribe();

        retry_forever_with_backoff_until_cancelled_returning(
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

                    let mut builder = LargeStringBuilder::new();
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
                        for value in page {
                            builder.append_value(value);
                        }
                    }

                    transaction.commit().await.streamling_with_context(|| {
                        format!("failed to finish cache load for table {full_table_name}")
                    })?;
                    Ok((updated_at, builder.finish(), pages_loaded))
                }
            },
            &operation_name,
            &mut shutdown,
        )
        .await
        .ok_or_else(|| {
            DynamicTableBackendError::Query(format!("{} cancelled by shutdown", operation_name))
        })
    }

    /// Returns true when the caller should run the freshness check now.
    /// `populated` gates the very first load: only once a cache exists may the
    /// debounce window suppress a check. The claim is a compare-and-swap on
    /// `last_freshness_check_ms`, so concurrent callers on the same batch yield
    /// at most one check per window instead of one per caller.
    fn try_claim_freshness_window(&self, now_ms: u64, populated: bool) -> bool {
        if self.cache_refresh_debounce_ms == 0 {
            return true;
        }
        let last = self.last_freshness_check_ms.load(Ordering::Relaxed);
        // Only debounce once a cache exists — the first load must always run.
        // ponytail: unpopulated callers all claim, so a burst before the first
        // populate can run duplicate `SELECT MAX` probes (bounded by the
        // populate window); refresh_cache's write-lock watermark re-check
        // prevents duplicate page loads. Single-flight first load would need
        // cross-task wait machinery that costs more than the duplicates.
        if populated && now_ms.saturating_sub(last) < self.cache_refresh_debounce_ms {
            return false;
        }
        if populated
            && self
                .last_freshness_check_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            // Another caller claimed this window; the populated cache it will
            // refresh stays valid for our probe.
            return false;
        }
        self.last_freshness_check_ms
            .store(now_ms, Ordering::Relaxed);
        true
    }

    async fn refresh_cache(&self, pool: Arc<PgPool>) -> Result<(), DynamicTableBackendError> {
        let cache = self
            .cache
            .as_ref()
            .expect("cache is present when refresh_cache is called");
        let populated = cache.read().await.is_some();
        let now_ms = self.created_at.elapsed().as_millis() as u64;
        if !self.try_claim_freshness_window(now_ms, populated) {
            return Ok(());
        }
        let freshness_check_started_at = Instant::now();
        let updated_at = self.latest_update(pool.clone()).await?;
        let freshness_check_ms = freshness_check_started_at.elapsed().as_millis() as u64;
        if freshness_check_ms >= 200 {
            warn!(
                table = %self.full_table_name,
                freshness_check_ms,
                debounce_ms = self.cache_refresh_debounce_ms,
                "Slow dynamic table cache freshness check: the time column may be missing an index"
            );
        }

        if let Some(cached) = cache.read().await.as_ref()
            && cached.updated_at == updated_at
        {
            return Ok(());
        }

        let mut cached = cache.write().await;
        if let Some(current) = cached.as_ref()
            && current.updated_at == updated_at
        {
            return Ok(());
        }

        let updated_since = cached
            .as_ref()
            .and_then(|current| current.updated_at.as_deref())
            .map(str::to_owned);
        let load_started_at = Instant::now();
        let (updated_at, keys, pages_loaded) =
            self.load_cache(pool, updated_since.as_deref()).await?;
        let elapsed_ms = load_started_at.elapsed().as_millis();
        let added_entries = keys.len();

        if let Some(current) = cached.as_mut() {
            current.append(updated_at, keys)?;
            debug!(
                table = %self.full_table_name,
                added_entries,
                total_entries = current.values.len(),
                pages_loaded,
                elapsed_ms = ?elapsed_ms,
                freshness_check_ms,
                previous_watermark = ?updated_since.as_deref(),
                watermark = ?current.updated_at.as_deref(),
                "Refreshed PostgreSQL dynamic table cache"
            );
        } else {
            let values = ArrowKeySet::from_keys(keys).map_err(DynamicTableBackendError::Query)?;
            let cache = PostgresDynamicTableCache { updated_at, values };
            info!(
                table = %self.full_table_name,
                total_entries = cache.values.len(),
                pages_loaded,
                elapsed_ms = ?elapsed_ms,
                freshness_check_ms,
                watermark = ?cache.updated_at.as_deref(),
                "Populated PostgreSQL dynamic table cache"
            );
            *cached = Some(cache);
        }

        Ok(())
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
            let err = classify_sqlx_error(
                &format!(
                    "Failed to validate cache time column '{}'",
                    self.time_column_name
                ),
                &self.full_table_name,
                &e,
            );
            error!("{}", err);
            err
        })?;

        Ok(())
    }

    /// Check if a batch of values exist in the table (internal method that doesn't split batches)
    /// Retries with exponential backoff until it succeeds or shutdown is requested — SQL
    /// transforms reach this through the dynamic-table UDF via `block_in_place`, so an
    /// uncancellable retry loop here used to pin the drain forever against a sick backend. Statement timeout bounds individual queries.
    /// Uses Arc to wrap values so retry clones are cheap (reference count increment only).
    async fn contains_batch(
        &self,
        pool: Arc<PgPool>,
        value_indices: Vec<(usize, String)>,
    ) -> Result<HashSet<Box<str>>, DynamicTableBackendError> {
        if value_indices.is_empty() {
            return Ok(HashSet::new());
        }

        // Wrap in Arc once before the retry loop to avoid cloning Vec on each retry
        let value_indices: Arc<[(usize, String)]> = Arc::from(value_indices.into_boxed_slice());
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let column_name: Arc<str> = Arc::from(self.column_name.as_str());
        let operation_name = format!("DynamicTable contains_batch ({})", self.full_table_name);
        let mut shutdown = crate::shutdown::subscribe();

        retry_forever_with_backoff_until_cancelled_returning(
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
            &mut shutdown,
        )
        .await
        .ok_or_else(|| {
            DynamicTableBackendError::Query(format!("{} cancelled by shutdown", operation_name))
        })
    }

    /// Append a batch of values to the table (internal method that doesn't split
    /// batches). Retries forever with exponential backoff — it only returns when
    /// the batch is committed (or the task is torn down), so callers may treat
    /// its return as commit-success when deciding whether to mirror the keys
    /// into the in-memory cache. Statement timeout prevents individual queries
    /// from hanging. Uses Arc to wrap values so retry clones are cheap
    /// (reference count increment only).
    async fn append_batch(
        &self,
        pool: Arc<PgPool>,
        values: Vec<String>,
    ) -> Result<(), DynamicTableBackendError> {
        if values.is_empty() {
            return Ok(());
        }

        let values_len = values.len();
        // Wrap in Arc once before the retry loop to avoid cloning Vec on each retry
        let values: Arc<[String]> = Arc::from(values.into_boxed_slice());
        let full_table_name: Arc<str> = Arc::from(self.full_table_name.as_str());
        let column_name: Arc<str> = Arc::from(self.column_name.as_str());
        let time_column_name: Arc<str> = Arc::from(self.time_column_name.as_str());
        let serialize_write = self.cache.is_some();
        let operation_name = format!("DynamicTable append_batch ({})", self.full_table_name);
        let mut shutdown = crate::shutdown::subscribe();

        retry_forever_with_backoff_until_cancelled_returning(
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
            &mut shutdown,
        )
        .await
        .ok_or_else(|| {
            DynamicTableBackendError::Query(format!("{} cancelled by shutdown", operation_name))
        })?;

        trace!(
            "[append_batch] for table name '{}' with {} values",
            self.full_table_name, values_len
        );
        Ok(())
    }
    /// Ensure the pool is initialized and table exists. Called lazily on first use.
    ///
    /// The whole init attempt (connect + table/index setup + cache validation)
    /// is retried forever with backoff while errors are retriable
    /// (`Connection`: failed connects, transient Postgres outages) so a
    /// temporarily unavailable database never fails the batch/pipeline.
    /// Permanent configuration errors (`Initialization`: invalid identifiers,
    /// missing column, non-orderable time column) fail fast. Each attempt is
    /// bounded: sqlx's acquire timeout bounds connect, the statement timeout
    /// bounds every query.
    async fn get_pool(&self) -> Result<Arc<PgPool>, DynamicTableBackendError> {
        debug!(
            "Initializing PostgreSQL connection pool for dynamic table: {}",
            self.full_table_name
        );
        self.pool
            .get_or_try_init(|| async {
                let operation_name = format!("DynamicTable init pool ({})", self.full_table_name);
                retry_if_retriable(
                    || async { self.try_init_pool().await.map_err(StreamlingError::from) },
                    &operation_name,
                )
                .await
                .map_err(|e| {
                    e.inner()
                        .downcast_ref::<DynamicTableBackendError>()
                        .cloned()
                        .unwrap_or_else(|| DynamicTableBackendError::Initialization(e.to_string()))
                })
            })
            .await
            .cloned()
    }

    /// One attempt of lazy pool init: connect, ensure the table (and index)
    /// exist, validate the cache time column. Connection errors are retried
    /// by [`Self::get_pool`]; Initialization errors are permanent.
    async fn try_init_pool(&self) -> Result<Arc<PgPool>, DynamicTableBackendError> {
        // Initialize pool
        trace!(
            "Connecting to PostgreSQL for table: {}",
            self.full_table_name
        );

        let connect_options = PgConnectOptions::from_str(self.config.connection_url().as_str())
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
                        format!("SET statement_timeout = {}", statement_timeout_ms).as_str(),
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
            error!(
                "Failed table-existence check for {}: {}",
                self.full_table_name, e
            );
            // Pass through: the error already carries its transient/permanent
            // classification.
            e
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
            .await?;

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
            let index_name = build_time_column_index_name(bare_table, &self.time_column_name);
            let create_index_sql = format!(
                r#"CREATE INDEX IF NOT EXISTS "{}" ON {} ("{}")"#,
                index_name, self.full_table_name, self.time_column_name
            );

            let mut transaction = pool_arc.begin().await.map_err(|e| {
                let err =
                    classify_sqlx_error("Failed to begin transaction", &self.full_table_name, &e);
                error!("{}", err);
                err
            })?;
            sqlx::query(&create_table_sql)
                .execute(&mut *transaction)
                .await
                .map_err(|e| {
                    let err =
                        classify_sqlx_error("Failed to create table", &self.full_table_name, &e);
                    error!("{}", err);
                    err
                })?;
            sqlx::query(&create_index_sql)
                .execute(&mut *transaction)
                .await
                .map_err(|e| {
                    let err = classify_sqlx_error(
                        &format!("Failed to create index {index_name}"),
                        &self.full_table_name,
                        &e,
                    );
                    error!("{}", err);
                    err
                })?;
            transaction.commit().await.map_err(|e| {
                let err =
                    classify_sqlx_error("Failed to commit transaction", &self.full_table_name, &e);
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

        Ok(pool_arc)
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
    // FNV-1a rather than std's DefaultHasher: the doc contract below requires
    // the suffix to be IDENTICAL across restarts and toolchain bumps so
    // `CREATE INDEX IF NOT EXISTS` stays idempotent, and DefaultHasher makes
    // no cross-version stability guarantee. FNV-1a is a fixed algorithm.
    let hash = fnv1a64(raw.as_bytes());
    // '_' + 16 hex hash chars.
    let suffix = format!("_{:016x}", hash);
    let prefix_len = MAX_IDENT_BYTES - suffix.len();
    format!("{}_{}", &raw[..prefix_len], &suffix[1..])
}

/// Deterministic FNV-1a 64-bit over the raw name bytes; used only to keep
/// generated identifier suffixes stable across processes and toolchains.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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

        // Keep this pipeline's own writes visible without waiting for the next
        // freshness check: insert the keys we just committed to postgres into the
        // cached set. Safe because `append_batch` retries forever until the batch
        // commits (see its doc), so any key reaching this point is persisted —
        // this can never produce a false positive. `updated_at` is deliberately
        // left untouched, so the next real refresh still fetches everything since
        // the old watermark (re-fetching our own rows is idempotent — `extend_from`
        // probes before extending, so re-fetched keys add no bytes).
        let appended_keys = self
            .cache
            .as_ref()
            .map(|_| LargeStringArray::from_iter(values.iter().map(|v| Some(v.as_str()))));
        if values_len <= self.max_batch_size {
            // Single batch - process directly
            self.append_batch(pool, values).await?;
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
            for result in join_all(futures).await {
                result?;
            }
        }

        trace!(
            "[append] completed for table name '{}' with {} values",
            self.full_table_name, values_len
        );
        if let (Some(cache), Some(appended)) = (&self.cache, appended_keys) {
            let mut cached = cache.write().await;
            if let Some(current) = cached.as_mut() {
                let appended_count = appended.len();
                current
                    .values
                    .extend_from(appended)
                    .map_err(DynamicTableBackendError::Query)?;
                trace!(
                    table = %self.full_table_name,
                    appended = appended_count,
                    total_entries = current.values.len(),
                    "Added own appended keys to PostgreSQL dynamic table cache"
                );
            }
        }

        Ok(())
    }

    async fn contains(&self, values: ArrayRef) -> Result<ArrayRef, DynamicTableBackendError> {
        trace!("contains() called for table: {}", self.full_table_name);

        // Cast ArrayRef to StringArray
        let string_array = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DynamicTableBackendError::StringArrayExpected)?;

        // Nothing to look up: membership is all-miss (nulls stay null)
        // independent of cache state, so return before acquiring the pool or
        // running the cached path's freshness check. Dataset sources emit
        // empty heartbeat batches continuously; running refresh_cache on each
        // turned the cached path into one `SELECT MAX` round trip per
        // heartbeat (~117x more round trips than the uncached path in a live
        // A/B at debounce=0).
        if string_array.null_count() == string_array.len() {
            return Ok(Arc::new(BooleanArray::new_null(string_array.len())));
        }

        let pool = self.get_pool().await.map_err(|e| {
            error!(
                "Failed to get pool for table {} in contains(): {}",
                self.full_table_name, e
            );
            e
        })?;

        if let Some(cache) = &self.cache {
            // Check for table changes on every non-empty invocation.
            self.refresh_cache(pool).await?;
            let cached = cache.read().await;
            let values = &cached
                .as_ref()
                .expect("cache is loaded after refresh_cache")
                .values;
            let result = values
                .contains_array(string_array)
                .map_err(DynamicTableBackendError::Query)?;
            return Ok(Arc::new(result));
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

        // Deduplicate values so each unique value is queried only once.
        let value_indices = deduplicate_value_indices(value_indices);

        let existing_set = if value_indices.len() <= self.max_batch_size {
            // Single batch - process directly
            self.contains_batch(pool, value_indices).await?
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
                combined_set.extend(chunk_set?);
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
            cache_refresh_debounce_ms: None,
        }
    }

    #[test]
    fn cache_append_preserves_existing_values() {
        let mut cache = PostgresDynamicTableCache {
            updated_at: Some("old".to_string()),
            values: ArrowKeySet::from_keys(LargeStringArray::from(vec!["existing"]))
                .expect("build initial cache"),
        };

        cache
            .append(
                Some("new".to_string()),
                LargeStringArray::from(vec!["appended"]),
            )
            .expect("append delta");

        assert_eq!(cache.updated_at.as_deref(), Some("new"));
        assert_eq!(cache.values.len(), 2);
        let needles = StringArray::from(vec!["existing", "appended", "missing"]);
        let out = cache.values.contains_array(&needles).expect("probe");
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
    }

    #[test]
    fn transient_sqlx_errors_classify_as_connection() {
        // Lost connection / refused connection: transient infrastructure
        // failure, must be retried, not fatal.
        let io = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        assert!(matches!(
            classify_sqlx_error("op", "tbl", &io),
            DynamicTableBackendError::Connection(_)
        ));

        assert!(sqlx_error_is_transient(&sqlx::Error::PoolTimedOut));
        assert!(sqlx_error_is_transient(&sqlx::Error::PoolClosed));

        // PostgreSQL connection exceptions and shutdowns are transient.
        assert!(pg_code_is_transient("08000"));
        assert!(pg_code_is_transient("08006"));
        assert!(pg_code_is_transient("57P01"));
        assert!(pg_code_is_transient("53300"));

        // Permanent SQL errors (permissions, missing column, bad operator)
        // stay Initialization so init fails fast.
        assert!(!pg_code_is_transient("42501")); // insufficient privilege
        assert!(!pg_code_is_transient("42703")); // column does not exist
        assert!(!pg_code_is_transient("42883")); // operator does not exist

        assert!(matches!(
            classify_sqlx_error("op", "tbl", &sqlx::Error::ColumnNotFound("x".into())),
            DynamicTableBackendError::Initialization(_)
        ));
    }

    /// The user-facing error chain must carry the diagnostic detail, not just
    /// the variant name (prod logs showed a bare `Caused by: Initialization`).
    #[test]
    fn backend_error_display_includes_detail() {
        let err = DynamicTableBackendError::Initialization("bad column 'x'".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Initialization"), "got: {msg}");
        assert!(msg.contains("bad column 'x'"), "got: {msg}");
    }

    /// Connection errors convert to retriable StreamlingErrors; Initialization
    /// errors stay non-retriable (fail fast).
    #[test]
    fn streamling_error_flags_follow_variant() {
        let conn = StreamlingError::from(DynamicTableBackendError::Connection(
            "server closed".to_string(),
        ));
        assert!(conn.is_retriable());

        let init = StreamlingError::from(DynamicTableBackendError::Initialization(
            "missing column".to_string(),
        ));
        assert!(!init.is_retriable());
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
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert!(disabled.cache.is_none());

        let enabled_factory = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let missing_time_column = enabled_factory
            .create_backend(
                "missing_time_column".to_string(),
                None,
                None,
                None,
                1000,
                0,
                None,
            )
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
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert!(cached.cache.is_some());
    }

    /// An empty or all-null batch has nothing to look up, so `contains` must
    /// return before any database work: no pool init, no cache freshness
    /// check. The unreachable host makes any DB touch error out, so an `Ok`
    /// result proves the early return happened. Dataset sources emit empty
    /// heartbeat batches continuously; before the early return, the cached
    /// path ran a `SELECT MAX` freshness round trip on every one of them
    /// (~117x more round trips than the uncached path in a live A/B).
    #[tokio::test]
    async fn cached_contains_skips_db_on_empty_or_all_null_batches() {
        let mut config = postgres_config(true);
        config.host = "127.0.0.1".to_string();
        config.port = 1; // nothing listens here: every connect fails fast
        let factory =
            PostgresDynamicTableBackendFactory::new(config).expect("factory should be valid");
        let backend = factory
            .create_backend(
                "empty_batch_test".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                None,
            )
            .await
            .expect("backend construction is lazy and must not connect");

        // Empty batch: zero-length result, no round trip.
        let empty: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let out = backend
            .contains(empty)
            .await
            .expect("empty batch must not touch the database");
        assert_eq!(out.len(), 0);

        // All-null batch: nulls preserved, no round trip.
        let all_null: ArrayRef = Arc::new(StringArray::from(vec![
            Option::<&str>::None,
            Option::<&str>::None,
        ]));
        let out = backend
            .contains(all_null)
            .await
            .expect("all-null batch must not touch the database");
        assert_eq!(out.len(), 2);
        assert!(out.is_null(0) && out.is_null(1));

        // Control: a non-empty batch must still require the (unreachable)
        // database. Connection failures now retry forever, so that shows up
        // as a blocked call rather than an error.
        let non_empty: ArrayRef = Arc::new(StringArray::from(vec![Some("v")]));
        assert!(
            tokio::time::timeout(Duration::from_millis(1000), backend.contains(non_empty))
                .await
                .is_err(),
            "non-empty batch must attempt the database lookup"
        );
    }
    /// A failed initial connection must not surface as an error: the lazy
    /// pool init retries forever with backoff (same as the query paths), so
    /// `contains` keeps blocking until the database is reachable instead of
    /// failing the batch/pipeline. The unreachable host makes every connect
    /// attempt fail; sqlx internally retries for its 30s acquire timeout, so
    /// the 65s simulated timeout only elapses if our own retry loop is in
    /// charge (with paused time the wait costs no wall clock).
    #[tokio::test(start_paused = true)]
    async fn connect_failure_retries_instead_of_erroring() {
        let mut config = postgres_config(false);
        config.host = "127.0.0.1".to_string();
        config.port = 1; // nothing listens here: every connect attempt fails
        let factory =
            PostgresDynamicTableBackendFactory::new(config).expect("factory should be valid");
        let backend = factory
            .create_backend(
                "connect_retry_test".to_string(),
                None,
                None,
                None,
                1000,
                0,
                None,
            )
            .await
            .expect("backend construction is lazy and must not connect");

        let batch: ArrayRef = Arc::new(StringArray::from(vec![Some("v")]));
        let result = tokio::time::timeout(Duration::from_secs(65), backend.contains(batch)).await;
        assert!(
            result.is_err(),
            "connection failure must be retried, not returned as an error (got {result:?})"
        );
    }
    #[tokio::test]
    async fn debounce_zero_passthrough_stays_zero() {
        // Defaulting to DEFAULT_CACHE_REFRESH_DEBOUNCE_MS happens in
        // DynamicTableBackendFactory::create; create_backend receives the
        // resolved value and must pass 0 through unchanged.
        let factory = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let backend = factory
            .create_backend(
                "debounce_default".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert_eq!(backend.cache_refresh_debounce_ms, 0);
    }

    #[tokio::test]
    async fn freshness_window_claim_yields_at_most_one_check_per_window() {
        let factory = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let backend = factory
            .create_backend(
                "freshness_claim_test".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                1000, // 1s window
                None,
            )
            .await
            .expect("backend should be valid");

        // The very first load runs regardless of the window (cache unpopulated).
        assert!(backend.try_claim_freshness_window(0, false));
        // Inside the window with a populated cache: suppressed.
        assert!(!backend.try_claim_freshness_window(500, true));
        // A concurrent caller inside the window is also suppressed.
        assert!(!backend.try_claim_freshness_window(600, true));
        // Window elapsed: allowed again; the claim advances the window.
        assert!(backend.try_claim_freshness_window(2000, true));
        assert!(!backend.try_claim_freshness_window(2100, true));
        assert!(backend.try_claim_freshness_window(3200, true));

        // Debounce disabled: every caller refreshes.
        let zero = factory
            .create_backend(
                "freshness_claim_zero".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert!(zero.try_claim_freshness_window(0, true));
        assert!(zero.try_claim_freshness_window(1, true));
    }
    #[tokio::test]
    async fn debounce_config_is_plumbed() {
        let factory = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let backend = factory
            .create_backend(
                "debounce_plumbed".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                5000,
                None,
            )
            .await
            .expect("backend should be valid");
        assert_eq!(backend.cache_refresh_debounce_ms, 5000);
    }

    #[tokio::test]
    async fn cache_resolution_falls_back_to_global_unless_overridden() {
        // Topology override wins over the global flag.
        let global_off = PostgresDynamicTableBackendFactory::new(postgres_config(false))
            .expect("factory should be valid");
        let forced_on = global_off
            .create_backend(
                "forced_on".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                Some(true),
            )
            .await
            .expect("backend should be valid");
        assert!(forced_on.cache.is_some());

        let global_on = PostgresDynamicTableBackendFactory::new(postgres_config(true))
            .expect("factory should be valid");
        let forced_off = global_on
            .create_backend(
                "forced_off".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                Some(false),
            )
            .await
            .expect("backend should be valid");
        assert!(forced_off.cache.is_none());

        // No override: falls back to the global flag.
        let defaulted_on = global_on
            .create_backend(
                "defaulted_on".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert!(defaulted_on.cache.is_some());

        let defaulted_off = global_off
            .create_backend(
                "defaulted_off".to_string(),
                None,
                None,
                Some("updated_at".to_string()),
                1000,
                0,
                None,
            )
            .await
            .expect("backend should be valid");
        assert!(defaulted_off.cache.is_none());
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

        // Pin the exact suffix: the hash must be FNV-1a (a fixed algorithm),
        // NOT std's DefaultHasher, which gives no cross-toolchain stability
        // guarantee — a different suffix after a Rust upgrade would make
        // `CREATE INDEX IF NOT EXISTS` create a duplicate index.
        assert_eq!(
            name,
            "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_54465ea85ad5121b"
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
