use datafusion::arrow::array::Array;
use std::sync::Arc;
use std::time::Duration;
use streamling_core::error::{Result, ResultExt};
use streamling_core::retry::{RetryOutcome, retry_forever_with_backoff_until_cancelled};
use streamling_core::streamling_err;
use streamling_core::utils::pg::execute_bounded;

use crate::table_providers::postgres::value_binding;

/// Upper bound for a single INSERT/DELETE attempt. Attempts are retried (with
/// shutdown-aware backoff) on timeout; both operations are idempotent.
const PER_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Context for sink operations, providing identifying information for error messages
#[derive(Clone)]
pub struct SinkContext {
    pub sink_name: String,
    pub schema_name: String,
    pub table_name: String,
}

/// Execute a batch insert query with retry logic.
/// Retries indefinitely until success — unless process shutdown is requested,
/// in which case it stops between attempts and returns an error so the sink
/// fails instead of acking a checkpoint for data that was never written.
/// If checkpoint_epoch is provided, binds the epoch value for each row.
pub async fn execute_batch_insert(
    pool: &sqlx::PgPool,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    column_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
    checkpoint_epoch: Option<u64>,
    client_statement_timeout: Option<Duration>,
) -> Result<()> {
    let query = query.to_string();
    let pool = pool.clone();
    let batch_columns: Vec<_> = batch_columns.to_vec();
    let column_indices = column_indices.to_vec();

    let operation_name = format!(
        "PostgreSQL INSERT into {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    let mut shutdown = streamling_core::shutdown::subscribe();
    let outcome = retry_forever_with_backoff_until_cancelled(
        move || {
            let query = query.clone();
            let pool = pool.clone();
            let batch_columns = batch_columns.clone();
            let column_indices = column_indices.clone();
            let checkpoint_epoch = checkpoint_epoch;
            async move {
                let mut q = sqlx::query(&query);
                for row_idx in 0..num_rows {
                    for &batch_col_idx in &column_indices {
                        let array = &batch_columns[batch_col_idx];
                        let data_type = array.data_type();
                        q = value_binding::bind_arrow_value_to_query(q, array, row_idx, data_type)
                            .streamling_context("failed to bind Arrow value to query")?;
                    }
                    if let Some(epoch) = checkpoint_epoch {
                        q = q.bind(epoch as i64);
                    }
                }
                // Bound each attempt so a hung connection cannot silently eat
                // the whole shutdown budget: cancellation is only checked
                // BETWEEN attempts, so an unbounded in-flight attempt would
                // leave the watchdog as the only way out. The configured
                // client-side statement timeout wins when set; otherwise a
                // fixed per-attempt bound applies. A timed-out attempt
                // discards its connection and is retried; the upsert is
                // idempotent.
                execute_bounded(
                    &pool,
                    q,
                    Some(client_statement_timeout.unwrap_or(PER_ATTEMPT_TIMEOUT)),
                )
                .await
                .streamling_context("failed to execute INSERT query")?;
                Ok(())
            }
        },
        &operation_name,
        &mut shutdown,
    )
    .await;

    match outcome {
        RetryOutcome::Completed => Ok(()),
        RetryOutcome::Cancelled => Err(streamling_err!(
            "{} aborted: shutdown requested before the write succeeded",
            operation_name
        )),
    }
}

/// Execute a batch delete query with retry logic.
/// Retries indefinitely until success — unless process shutdown is requested,
/// in which case it stops between attempts and returns an error so the sink
/// fails instead of acking a checkpoint for a delete that never applied.
pub async fn execute_batch_delete(
    pool: &sqlx::PgPool,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    primary_key_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
    client_statement_timeout: Option<Duration>,
) -> Result<()> {
    let query = query.to_string();
    let pool = pool.clone();
    let batch_columns: Vec<_> = batch_columns.to_vec();
    let primary_key_indices = primary_key_indices.to_vec();

    let operation_name = format!(
        "PostgreSQL DELETE from {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    let mut shutdown = streamling_core::shutdown::subscribe();
    let outcome = retry_forever_with_backoff_until_cancelled(
        move || {
            let query = query.clone();
            let pool = pool.clone();
            let batch_columns = batch_columns.clone();
            let primary_key_indices = primary_key_indices.clone();
            async move {
                let mut q = sqlx::query(&query);
                for row_idx in 0..num_rows {
                    for &pk_idx in &primary_key_indices {
                        let array = &batch_columns[pk_idx];
                        let data_type = array.data_type();
                        q = value_binding::bind_arrow_value_to_query(q, array, row_idx, data_type)
                            .streamling_context("failed to bind Arrow value to query")?;
                    }
                }
                // See the INSERT path: bound each attempt so a hung connection
                // can't eat the shutdown budget. Deletes by primary key are
                // idempotent, so a timed-out attempt is safely retried.
                execute_bounded(
                    &pool,
                    q,
                    Some(client_statement_timeout.unwrap_or(PER_ATTEMPT_TIMEOUT)),
                )
                .await
                .streamling_context("failed to execute DELETE query")?;
                Ok(())
            }
        },
        &operation_name,
        &mut shutdown,
    )
    .await;

    match outcome {
        RetryOutcome::Completed => Ok(()),
        RetryOutcome::Cancelled => Err(streamling_err!(
            "{} aborted: shutdown requested before the delete succeeded",
            operation_name
        )),
    }
}

// Note: Comprehensive testing of batch execution is done via integration tests
// in `crates/streamling/tests/pipeline_postgres_sink.rs` which verify that batches
// are correctly inserted into PostgreSQL with proper retry logic. The client-side
// statement bound itself is unit-tested next to its implementation in
// `streamling_core::utils::pg` (fake silent-server reproduction) and covered
// end-to-end by `streamling-e2e/tests/postgres_sink_recovery.rs`.
