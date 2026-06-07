use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use std::sync::Arc;

/// Execute an async function on N slices of a single RecordBatch concurrently.
///
/// - `batch`: The big combined RecordBatch (already batch by caller)
/// - `parallelism`: Number of slices/parallel tasks (N)
/// - `min_batch_size`: Minimum number of rows to execute in each slice. If less workers are able to process the whole batch, we will use less workers.
/// - `execute_fn`: Async function called once per slice
///
/// Slices are formed as equally as possible using ceil(max_total_rows / parallelism).
pub async fn parallel_execute<F, Fut>(
    batch: &RecordBatch,
    parallelism: usize,
    min_batch_e: usize,
    execute_fn: F,
) where
    F: Fn(RecordBatch) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let total_rows = batch.num_rows();
    if total_rows == 0 {
        return;
    }

    let max_workers = parallelism.max(1);
    let target = min_batch_e.max(1);
    let workers_by_target = total_rows.div_ceil(target);
    let effective_workers = max_workers.min(workers_by_target.max(1));
    let shard_capacity = total_rows.div_ceil(effective_workers);

    let mut slices: Vec<RecordBatch> = Vec::with_capacity(effective_workers);
    for i in 0..effective_workers {
        let start = i.saturating_mul(shard_capacity);
        let remaining = total_rows.saturating_sub(start);
        let len = remaining.min(shard_capacity);
        let offset = start.min(total_rows);
        slices.push(batch.slice(offset, len));
    }

    let func: Arc<F> = Arc::new(execute_fn);
    let tasks = futures::stream::iter(slices).map(move |slice| {
        let func = func.clone();
        (func)(slice)
    });

    let _ = tasks
        .buffer_unordered(effective_workers)
        .collect::<Vec<_>>()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let ids: Vec<i32> = (0..rows as i32).collect();
        let names: Vec<String> = (0..rows).map(|i| format!("name_{}", i)).collect();
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    async fn inc_seen_assert(seen: Arc<AtomicUsize>, slice: RecordBatch, shard_cap: usize) {
        assert!(slice.num_rows() <= shard_cap);
        seen.fetch_add(slice.num_rows(), std::sync::atomic::Ordering::SeqCst);
    }

    async fn inc_seen(seen: Arc<AtomicUsize>, slice: RecordBatch) {
        seen.fetch_add(slice.num_rows(), std::sync::atomic::Ordering::SeqCst);
    }

    #[tokio::test]
    async fn parallel_execute_exact_division() {
        let batch = make_batch(1000);
        let parallelism = 4;
        let shard_cap = batch.num_rows().div_ceil(parallelism);
        let seen = Arc::new(AtomicUsize::new(0));

        parallel_execute(&batch, parallelism, shard_cap, {
            let seen = seen.clone();
            move |slice| inc_seen_assert(seen.clone(), slice, shard_cap)
        })
        .await;

        assert_eq!(seen.load(Ordering::SeqCst), 1000);
    }

    #[tokio::test]
    async fn parallel_execute_with_remainder() {
        let batch = make_batch(1025);
        let parallelism = 4;
        let shard_cap = batch.num_rows().div_ceil(parallelism);
        let seen = Arc::new(AtomicUsize::new(0));

        parallel_execute(&batch, parallelism, 250, {
            let seen = seen.clone();
            move |slice| inc_seen_assert(seen.clone(), slice, shard_cap)
        })
        .await;

        assert_eq!(seen.load(Ordering::SeqCst), 1025);
    }

    #[tokio::test]
    async fn parallel_execute_over_parallel() {
        let batch = make_batch(10);
        let parallelism = 32; // more workers than rows
        let seen = Arc::new(AtomicUsize::new(0));

        parallel_execute(&batch, parallelism, 1, {
            let seen = seen.clone();
            move |slice| inc_seen(seen.clone(), slice)
        })
        .await;

        assert_eq!(seen.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn parallel_execute_with_min_batch_size() {
        let batch = make_batch(10);
        let parallelism = 10;
        let seen = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));

        parallel_execute(&batch, parallelism, 100, {
            let seen = seen.clone();
            let count = count.clone();
            move |slice| {
                let seen = seen.clone();
                let count = count.clone();
                async move {
                    seen.fetch_add(slice.num_rows(), Ordering::SeqCst);
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
        .await;

        assert_eq!(seen.load(Ordering::SeqCst), 10);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
