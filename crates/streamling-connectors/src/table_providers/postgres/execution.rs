use datafusion::arrow::array::Array;
use std::sync::Arc;
use std::time::Duration;
use streamling_config::PostgresSinkConfig;
use streamling_core::error::ResultExt;
use streamling_core::utils::pg::PostgresConnection;
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::table_providers::postgres::value_binding;

/// A shared pool that can be atomically replaced on failover.
/// The u64 is a generation counter — incremented each time the pool is replaced.
pub type SharedPool = Arc<Mutex<(sqlx::PgPool, u64)>>;

/// Context for sink operations, providing identifying information for error messages
#[derive(Clone)]
pub struct SinkContext {
    pub sink_name: String,
    pub schema_name: String,
    pub table_name: String,
}

fn is_read_only_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        return db_err.code().as_deref() == Some("25006");
    }
    false
}

/// Execute a batch insert query with retry logic.
/// Never returns an error as retries continue indefinitely until success.
/// If checkpoint_epoch is provided, binds the epoch value for each row.
pub async fn execute_batch_insert(
    pool: &SharedPool,
    config: &PostgresSinkConfig,
    parallelism: usize,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    column_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
    checkpoint_epoch: Option<u64>,
) {
    let operation_name = format!(
        "PostgreSQL INSERT into {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = 100;

    loop {
        attempt = attempt.saturating_add(1);

        let (current_pool, my_gen) = {
            let g = pool.lock().await;
            (g.0.clone(), g.1)
        };

        let result = {
            let mut q = sqlx::query(query);
            let bind_result: std::result::Result<_, _> = (|| {
                for row_idx in 0..num_rows {
                    for &batch_col_idx in column_indices {
                        let array = &batch_columns[batch_col_idx];
                        let data_type = array.data_type();
                        q = value_binding::bind_arrow_value_to_query(q, array, row_idx, data_type)
                            .streamling_context("failed to bind Arrow value to query")?;
                    }
                    if let Some(epoch) = checkpoint_epoch {
                        q = q.bind(epoch as i64);
                    }
                }
                Ok(q)
            })();

            match bind_result {
                Ok(q) => q.execute(&current_pool).await,
                Err(e) => {
                    error!("[{}] bind error: {:?}", operation_name, e);
                    Err(sqlx::Error::Protocol(format!("{:?}", e)))
                }
            }
        };

        match result {
            Ok(_) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                return;
            }
            Err(e) if is_read_only_error(&e) => {
                handle_read_only_error(pool, config, parallelism, my_gen, &operation_name).await;
            }
            Err(e) => {
                if attempt > 5 {
                    error!(
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name, attempt, e
                    );
                } else {
                    warn!(
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name, attempt, e
                    );
                }
            }
        }

        let jitter = (attempt as u64 % 100) * 7;
        let sleep_ms = (backoff_ms + jitter).min(30_000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        backoff_ms = backoff_ms.saturating_mul(2).min(30_000);
    }
}

/// Execute a batch delete query with retry logic.
/// Never returns an error as retries continue indefinitely until success.
pub async fn execute_batch_delete(
    pool: &SharedPool,
    config: &PostgresSinkConfig,
    parallelism: usize,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    primary_key_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
) {
    let operation_name = format!(
        "PostgreSQL DELETE from {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = 100;

    loop {
        attempt = attempt.saturating_add(1);

        let (current_pool, my_gen) = {
            let g = pool.lock().await;
            (g.0.clone(), g.1)
        };

        let result = {
            let mut q = sqlx::query(query);
            let bind_result: std::result::Result<_, _> = (|| {
                for row_idx in 0..num_rows {
                    for &pk_idx in primary_key_indices {
                        let array = &batch_columns[pk_idx];
                        let data_type = array.data_type();
                        q = value_binding::bind_arrow_value_to_query(q, array, row_idx, data_type)
                            .streamling_context("failed to bind Arrow value to query")?;
                    }
                }
                Ok(q)
            })();

            match bind_result {
                Ok(q) => q.execute(&current_pool).await,
                Err(e) => {
                    error!("[{}] bind error: {:?}", operation_name, e);
                    Err(sqlx::Error::Protocol(format!("{:?}", e)))
                }
            }
        };

        match result {
            Ok(_) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                return;
            }
            Err(e) if is_read_only_error(&e) => {
                handle_read_only_error(pool, config, parallelism, my_gen, &operation_name).await;
            }
            Err(e) => {
                if attempt > 5 {
                    error!(
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name, attempt, e
                    );
                } else {
                    warn!(
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name, attempt, e
                    );
                }
            }
        }

        let jitter = (attempt as u64 % 100) * 7;
        let sleep_ms = (backoff_ms + jitter).min(30_000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        backoff_ms = backoff_ms.saturating_mul(2).min(30_000);
    }
}

/// Handle a READ_ONLY error by performing a deduplicated pool swap.
/// If this task is the first to detect the stale generation, it closes the old pool
/// and creates a new one. Otherwise it logs that another task already refreshed.
async fn handle_read_only_error(
    pool: &SharedPool,
    config: &PostgresSinkConfig,
    parallelism: usize,
    my_gen: u64,
    operation_name: &str,
) {
    let mut guard = pool.lock().await;
    if guard.1 == my_gen {
        warn!(
            "[{}] READ_ONLY error (SQLSTATE 25006) — pool is stale after failover; recreating (gen {} → {})",
            operation_name, my_gen, my_gen + 1
        );
        guard.0.close().await;
        match PostgresConnection::new_with_parallelism(config, parallelism).await {
            Ok(conn) => {
                guard.0 = conn.pool().clone();
                guard.1 = my_gen + 1;
            }
            Err(e) => {
                warn!(
                    "[{}] Failed to recreate pool: {:?}; will retry",
                    operation_name, e
                );
            }
        }
    } else {
        warn!(
            "[{}] READ_ONLY error but pool already refreshed (gen {}); retrying with new pool",
            operation_name, guard.1
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_read_only_error_with_25006() {
        let err = sqlx::Error::Database(Box::new(TestDbError {
            code: Some("25006".into()),
            message: "cannot execute INSERT in a read-only transaction".into(),
        }));
        assert!(is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_other_code() {
        let err = sqlx::Error::Database(Box::new(TestDbError {
            code: Some("23505".into()),
            message: "duplicate key value violates unique constraint".into(),
        }));
        assert!(!is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_non_db_error() {
        let err = sqlx::Error::PoolTimedOut;
        assert!(!is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_no_code() {
        let err = sqlx::Error::Database(Box::new(TestDbError {
            code: None,
            message: "some error".into(),
        }));
        assert!(!is_read_only_error(&err));
    }

    /// A minimal implementation of sqlx::error::DatabaseError for testing.
    #[derive(Debug)]
    struct TestDbError {
        code: Option<String>,
        message: String,
    }

    impl std::fmt::Display for TestDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl sqlx::error::DatabaseError for TestDbError {
        fn message(&self) -> &str {
            &self.message
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code.as_deref().map(std::borrow::Cow::Borrowed)
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
}
