use datafusion::arrow::array::Array;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use streamling_config::PostgresSinkConfig;
use streamling_core::error::{ResultExt, StreamlingError};
use streamling_core::retry::retry_forever_with_backoff_async_on_error;
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

fn is_read_only_error(err: &StreamlingError) -> bool {
    // The underlying sqlx::Error is wrapped inside the StreamlingError cause
    // chain (execute/bind errors are context-wrapped before reaching here), so
    // walk the chain to find the database error and its SQLSTATE.
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(sqlx::Error::Database(db_err)) = e.downcast_ref::<sqlx::Error>() {
            return db_err.code().as_deref() == Some("25006");
        }
        current = e.source();
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

    // Generation observed by the in-flight attempt. The operation records the pool
    // generation it used; the on_error hook reads it to decide whether this task
    // owns the pool swap — deduplicating reconnection across concurrent writers.
    let observed_gen = AtomicU64::new(0);
    let observed_gen = &observed_gen;

    retry_forever_with_backoff_async_on_error(
        || async {
            let (current_pool, my_gen) = {
                let g = pool.lock().await;
                (g.0.clone(), g.1)
            };
            observed_gen.store(my_gen, Ordering::Relaxed);

            let mut q = sqlx::query(query);
            let bind_result: streamling_core::error::Result<_> = (|| {
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
            let q = match bind_result {
                Ok(q) => q,
                Err(e) => {
                    error!("[{}] bind error: {:?}", operation_name, e);
                    return Err(e);
                }
            };
            q.execute(&current_pool)
                .await
                .streamling_context("failed to execute INSERT query")?;
            Ok(())
        },
        |err: StreamlingError| {
            let operation_name = operation_name.clone();
            async move {
                if is_read_only_error(&err) {
                    handle_read_only_error(
                        pool,
                        config,
                        parallelism,
                        observed_gen.load(Ordering::Relaxed),
                        &operation_name,
                    )
                    .await;
                }
            }
        },
        &operation_name,
    )
    .await;
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

    let observed_gen = AtomicU64::new(0);
    let observed_gen = &observed_gen;

    retry_forever_with_backoff_async_on_error(
        || async {
            let (current_pool, my_gen) = {
                let g = pool.lock().await;
                (g.0.clone(), g.1)
            };
            observed_gen.store(my_gen, Ordering::Relaxed);

            let mut q = sqlx::query(query);
            let bind_result: streamling_core::error::Result<_> = (|| {
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
            let q = match bind_result {
                Ok(q) => q,
                Err(e) => {
                    error!("[{}] bind error: {:?}", operation_name, e);
                    return Err(e);
                }
            };
            q.execute(&current_pool)
                .await
                .streamling_context("failed to execute DELETE query")?;
            Ok(())
        },
        |err: StreamlingError| {
            let operation_name = operation_name.clone();
            async move {
                if is_read_only_error(&err) {
                    handle_read_only_error(
                        pool,
                        config,
                        parallelism,
                        observed_gen.load(Ordering::Relaxed),
                        &operation_name,
                    )
                    .await;
                }
            }
        },
        &operation_name,
    )
    .await;
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
    // Swap the pool and bump the generation while holding the lock, then drop the guard before
    // closing the old pool — close() waits for in-flight queries and must not block writers.
    let old_pool = {
        let mut guard = pool.lock().await;
        if guard.1 == my_gen {
            warn!(
                "[{}] READ_ONLY error (SQLSTATE 25006) — pool is stale after failover; recreating (gen {} → {})",
                operation_name,
                my_gen,
                my_gen + 1
            );
            match PostgresConnection::new_with_parallelism(config, parallelism).await {
                Ok(conn) => {
                    let old = std::mem::replace(&mut guard.0, conn.pool().clone());
                    guard.1 = my_gen + 1;
                    Some(old)
                }
                Err(e) => {
                    warn!(
                        "[{}] Failed to recreate pool: {:?}; will retry with existing pool",
                        operation_name, e
                    );
                    None
                }
            }
        } else {
            warn!(
                "[{}] READ_ONLY error but pool already refreshed (gen {}); retrying with new pool",
                operation_name, guard.1
            );
            None
        }
    };
    if let Some(old_pool) = old_pool {
        old_pool.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrap_sqlx(err: sqlx::Error) -> StreamlingError {
        // Mirror production: execute errors reach the retry loop wrapped in a
        // StreamlingError cause chain via streamling_context.
        Err::<(), sqlx::Error>(err)
            .streamling_context("failed to execute INSERT query")
            .unwrap_err()
    }

    #[test]
    fn test_is_read_only_error_with_25006() {
        let err = wrap_sqlx(sqlx::Error::Database(Box::new(TestDbError {
            code: Some("25006".into()),
            message: "cannot execute INSERT in a read-only transaction".into(),
        })));
        assert!(is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_other_code() {
        let err = wrap_sqlx(sqlx::Error::Database(Box::new(TestDbError {
            code: Some("23505".into()),
            message: "duplicate key value violates unique constraint".into(),
        })));
        assert!(!is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_non_db_error() {
        let err = wrap_sqlx(sqlx::Error::PoolTimedOut);
        assert!(!is_read_only_error(&err));
    }

    #[test]
    fn test_is_read_only_error_with_no_code() {
        let err = wrap_sqlx(sqlx::Error::Database(Box::new(TestDbError {
            code: None,
            message: "some error".into(),
        })));
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

    impl std::error::Error for TestDbError {}

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
