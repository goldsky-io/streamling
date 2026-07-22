use datafusion::arrow::array::Array;
use std::sync::Arc;
use std::time::Duration;
use streamling_core::error::{Result, ResultExt, StreamlingError};
use streamling_core::retry::retry_forever_with_backoff_async;

use crate::table_providers::postgres::value_binding;

/// Context for sink operations, providing identifying information for error messages
#[derive(Clone)]
pub struct SinkContext {
    pub sink_name: String,
    pub schema_name: String,
    pub table_name: String,
}

/// Execute a query on a pooled connection with a client-side time bound.
///
/// The server-side `statement_timeout` cannot fire when the connection is dead
/// (e.g. a NAT/firewall silently dropped the flow after the statement was
/// sent): the client just awaits a response that will never arrive, and sqlx
/// exposes no TCP keepalive to detect it. Without this bound such an await
/// hangs the sink forever with no logs.
///
/// On timeout the connection is detached from the pool and dropped rather
/// than returned: the next acquire would otherwise receive the same dead
/// socket, and sqlx's return-to-pool cleanup can itself block on it. Dropping
/// a detached connection closes the socket without any await, and the pool
/// opens a fresh replacement on the next acquire.
///
/// Delivery semantics: like every retried error in this sink (e.g. a
/// connection drop after the server committed but before the response
/// arrived), a timeout can re-execute a statement that already committed —
/// at-least-once. Upsert/delete statements absorb this; sinks configured
/// without a primary key issue plain INSERTs and can observe duplicates,
/// which is the sink's pre-existing contract, not a property of this bound.
async fn execute_bounded(
    pool: &sqlx::PgPool,
    query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    timeout: Duration,
) -> Result<()> {
    let mut conn = pool.acquire().await.map_err(|e| {
        StreamlingError::retriable_with_cause("failed to acquire PostgreSQL connection", e)
    })?;

    match tokio::time::timeout(timeout, query.execute(&mut *conn)).await {
        Ok(result) => {
            result.streamling_context("failed to execute statement")?;
            Ok(())
        }
        Err(_elapsed) => {
            drop(conn.detach());
            Err(StreamlingError::retriable(format!(
                "statement did not complete within {timeout:?}; discarded the connection and will retry on a fresh one"
            )))
        }
    }
}

/// Execute a batch insert query with retry logic
/// Never returns an error as retries continue indefinitely until success
/// If checkpoint_epoch is provided, binds the epoch value for each row
pub async fn execute_batch_insert(
    pool: &sqlx::PgPool,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    column_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
    checkpoint_epoch: Option<u64>,
    client_statement_timeout: Duration,
) {
    let query = query.to_string();
    let pool = pool.clone();
    let batch_columns: Vec<_> = batch_columns.to_vec();
    let column_indices = column_indices.to_vec();

    let operation_name = format!(
        "PostgreSQL INSERT into {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    retry_forever_with_backoff_async(
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
                execute_bounded(&pool, q, client_statement_timeout)
                    .await
                    .streamling_context("failed to execute INSERT query")?;
                Ok(())
            }
        },
        &operation_name,
    )
    .await
}

/// Execute a batch delete query with retry logic
/// Never returns an error as retries continue indefinitely until success
pub async fn execute_batch_delete(
    pool: &sqlx::PgPool,
    query: &str,
    batch_columns: &[Arc<dyn Array>],
    primary_key_indices: &[usize],
    num_rows: usize,
    ctx: &SinkContext,
    client_statement_timeout: Duration,
) {
    let query = query.to_string();
    let pool = pool.clone();
    let batch_columns: Vec<_> = batch_columns.to_vec();
    let primary_key_indices = primary_key_indices.to_vec();

    let operation_name = format!(
        "PostgreSQL DELETE from {}.{} ({})",
        ctx.schema_name, ctx.table_name, ctx.sink_name
    );

    retry_forever_with_backoff_async(
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
                execute_bounded(&pool, q, client_statement_timeout)
                    .await
                    .streamling_context("failed to execute DELETE query")?;
                Ok(())
            }
        },
        &operation_name,
    )
    .await
}

// Note: Comprehensive testing of batch execution is done via integration tests
// in `crates/streamling/tests/pipeline_postgres_sink.rs` which verify that batches
// are correctly inserted into PostgreSQL with proper retry logic.

#[cfg(test)]
mod tests {
    use super::*;
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
    /// it must fail retriable within the timeout, discard the dead connection,
    /// and acquire a fresh one on the next attempt.
    #[tokio::test]
    async fn test_bounded_execute_times_out_and_discards_dead_connection() {
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

        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_bounded(&pool, sqlx::query("SELECT 1"), Duration::from_millis(300)),
        )
        .await
        .expect("execute_bounded must not hang on a silent server");

        let err = result.expect_err("must time out against a silent server");
        assert!(err.is_retriable(), "timeout must be retriable: {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "must fail within the client-side bound, took {:?}",
            started.elapsed()
        );
        assert_eq!(connections.load(Ordering::SeqCst), 1);

        // The dead connection must have been discarded: a second attempt gets
        // a fresh physical connection instead of the poisoned pooled one.
        let result2 = tokio::time::timeout(
            Duration::from_secs(10),
            execute_bounded(&pool, sqlx::query("SELECT 1"), Duration::from_millis(300)),
        )
        .await
        .expect("second attempt must not hang either");
        assert!(result2.is_err());
        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "expected a fresh physical connection after the discard"
        );
    }
}
