use crate::checkpoints::checkpoint_management::{
    CheckpointMessage, enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages,
    strip_checkpoint_messages,
};
use crate::utils::batch::enrich_batch_with_metadata;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use futures::StreamExt;
use futures::stream::Stream;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Default byte-size cap for `Utf8`/`Binary` columns in accumulated batches.
/// This is half of `i32::MAX` (~1 GiB), providing a safety margin against the
/// Arrow take kernel's 2 GiB offset overflow limit.
pub const DEFAULT_MAX_STRING_BYTES: usize = (i32::MAX as usize) / 2;

pub struct BatchAccumulator {
    batch_size: usize,
    batch_flush_interval: Option<Duration>,
    /// Maximum cumulative bytes in `Utf8`/`Binary`/`LargeUtf8`/`LargeBinary` columns
    /// before triggering a flush. Defaults to `None` (no byte-based flushing).
    max_string_bytes: Option<usize>,
    accumulated_batches: VecDeque<RecordBatch>,
    current_row_count: usize,
    /// Cached sum of string/binary buffer bytes across all accumulated batches.
    /// Avoids re-computing on every push.
    accumulated_string_bytes: usize,
    last_flush_time: Instant,
}

impl BatchAccumulator {
    pub fn new(batch_size: usize, batch_flush_interval: Option<Duration>) -> Self {
        Self::with_max_string_bytes(batch_size, batch_flush_interval, None)
    }

    /// Create a new `BatchAccumulator` with an optional byte-size cap for string/binary columns.
    ///
    /// When `max_string_bytes` is `Some(n)`, the accumulator will flush early if the cumulative
    /// byte size of `Utf8`/`Binary`/`LargeUtf8`/`LargeBinary` columns exceeds `n` bytes, even if
    /// the row count threshold hasn't been reached. This prevents the Arrow take kernel's 2 GiB
    /// offset overflow for wide string columns.
    ///
    /// Use [`DEFAULT_MAX_STRING_BYTES`] (~1 GiB) as the recommended threshold.
    pub fn with_max_string_bytes(
        batch_size: usize,
        batch_flush_interval: Option<Duration>,
        max_string_bytes: Option<usize>,
    ) -> Self {
        Self {
            batch_size,
            batch_flush_interval,
            max_string_bytes,
            accumulated_batches: VecDeque::new(),
            current_row_count: 0,
            accumulated_string_bytes: 0,
            last_flush_time: Instant::now(),
        }
    }

    /// Compute the total byte size of string/binary columns in a single batch.
    fn batch_string_bytes(batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|col| match col.data_type() {
                DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                    col.get_buffer_memory_size()
                }
                _ => 0,
            })
            .sum()
    }

    pub fn push(&mut self, batch: RecordBatch) -> Option<VecDeque<RecordBatch>> {
        // Check if batch has checkpoint metadata - empty batches with checkpoint data must be preserved
        let has_checkpoint_data =
            !extract_checkpoint_messages(batch.schema().metadata()).is_empty();

        if batch.num_rows() == 0 {
            // If empty batch has checkpoint metadata, add it to accumulated batches
            if has_checkpoint_data {
                self.accumulated_batches.push_back(batch);
            }
            return None;
        }

        self.current_row_count += batch.num_rows();
        self.accumulated_string_bytes += Self::batch_string_bytes(&batch);
        self.accumulated_batches.push_back(batch);

        if self.should_flush_by_size() {
            self.flush()
        } else {
            None
        }
    }

    pub fn should_flush_by_size(&self) -> bool {
        if self.current_row_count >= self.batch_size {
            return true;
        }
        // Also flush if string/binary bytes exceed the cap
        if let Some(max_bytes) = self.max_string_bytes {
            if self.accumulated_string_bytes >= max_bytes {
                return true;
            }
        }
        false
    }

    pub fn should_flush_by_time(&self) -> bool {
        match self.batch_flush_interval {
            Some(interval) => self.last_flush_time.elapsed() >= interval,
            None => false,
        }
    }

    pub fn check_time_flush(&mut self) -> Option<VecDeque<RecordBatch>> {
        if self.should_flush_by_time() && !self.accumulated_batches.is_empty() {
            self.flush()
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<VecDeque<RecordBatch>> {
        if self.accumulated_batches.is_empty() {
            return None;
        }

        let mut batches_to_return = VecDeque::new();
        let mut rows_included = 0;
        let mut bytes_returned = 0usize;
        let mut partial_batch = None;

        while let Some(batch) = self.accumulated_batches.pop_front() {
            let batch_rows = batch.num_rows();
            let batch_bytes = Self::batch_string_bytes(&batch);

            if rows_included + batch_rows <= self.batch_size {
                batches_to_return.push_back(batch);
                rows_included += batch_rows;
                bytes_returned += batch_bytes;
            } else {
                let rows_needed = self.batch_size - rows_included;
                if rows_needed > 0 {
                    let slice = batch.slice(0, rows_needed);
                    bytes_returned += Self::batch_string_bytes(&slice);
                    batches_to_return.push_back(slice);
                }

                let remaining_rows = batch_rows - rows_needed;
                if remaining_rows > 0 {
                    if rows_needed > 0 {
                        let remainder = batch.slice(rows_needed, remaining_rows);
                        partial_batch = Some(strip_checkpoint_messages(&remainder));
                    } else {
                        partial_batch = Some(batch);
                    }
                    self.current_row_count = remaining_rows;
                } else {
                    self.current_row_count = 0;
                }
                break;
            }
        }

        if let Some(batch) = partial_batch {
            // Recalculate remaining bytes from partial batch + any remaining accumulated batches
            let remaining_bytes = Self::batch_string_bytes(&batch)
                + self
                    .accumulated_batches
                    .iter()
                    .map(|b| Self::batch_string_bytes(b))
                    .sum::<usize>();
            self.accumulated_string_bytes = remaining_bytes;
            self.accumulated_batches.push_back(batch);
        } else if self.accumulated_batches.is_empty() {
            self.current_row_count = 0;
            self.accumulated_string_bytes = 0;
        } else {
            // Some batches remain but no partial batch was created
            self.accumulated_string_bytes = self
                .accumulated_batches
                .iter()
                .map(|b| Self::batch_string_bytes(b))
                .sum();
        }

        self.last_flush_time = Instant::now();

        if batches_to_return.is_empty() {
            None
        } else {
            Some(batches_to_return)
        }
    }

    pub fn flush_all(&mut self) -> Option<VecDeque<RecordBatch>> {
        if self.accumulated_batches.is_empty() {
            return None;
        }

        let batches = std::mem::take(&mut self.accumulated_batches);
        self.current_row_count = 0;
        self.accumulated_string_bytes = 0;
        self.last_flush_time = Instant::now();

        Some(batches)
    }

    pub fn flush_merged(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        match self.flush() {
            Some(batches) => merge_batches(batches),
            None => Ok(None),
        }
    }

    pub fn flush_all_merged(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        match self.flush_all() {
            Some(batches) => merge_batches(batches),
            None => Ok(None),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.accumulated_batches.is_empty()
    }

    pub fn current_row_count(&self) -> usize {
        self.current_row_count
    }
}

/// Merge a VecDeque of RecordBatches into a single RecordBatch.
///
/// Preserves all checkpoint messages from all batches. Only performs actual
/// data copying (via `concat_batches`) when there are multiple data batches;
/// a single data batch is returned directly.
///
/// Returns `Ok(None)` only when the input is empty.
pub fn merge_batches(
    batches: VecDeque<RecordBatch>,
) -> Result<Option<RecordBatch>, DataFusionError> {
    if batches.is_empty() {
        return Ok(None);
    }

    let mut all_checkpoint_messages: Vec<CheckpointMessage> = Vec::new();
    let mut data_batches: Vec<RecordBatch> = Vec::new();

    for batch in &batches {
        let messages = extract_checkpoint_messages(batch.schema().metadata());
        all_checkpoint_messages.extend(messages);

        if batch.num_rows() > 0 {
            data_batches.push(strip_checkpoint_messages(batch));
        }
    }

    let merged = if data_batches.is_empty() {
        // All batches were empty (checkpoint-only). Return an empty batch preserving schema.
        strip_checkpoint_messages(&batches[0])
    } else if data_batches.len() == 1 {
        data_batches
            .into_iter()
            .next()
            .expect("guaranteed by len == 1 check")
    } else {
        let schema = data_batches[0].schema();
        concat_batches(&schema, data_batches.iter())?
    };

    if all_checkpoint_messages.is_empty() {
        Ok(Some(merged))
    } else {
        let mut metadata: HashMap<String, String> = merged.schema().metadata().clone();
        enrich_batch_metadata_with_checkpoints(&mut metadata, &all_checkpoint_messages);
        Ok(Some(enrich_batch_with_metadata(merged, metadata)?))
    }
}

pub struct AsyncBatchAccumulator {
    accumulator: BatchAccumulator,
    batch_flush_interval: Option<Duration>,
    name: String,
}

impl AsyncBatchAccumulator {
    pub fn new(batch_size: usize, batch_flush_interval: Option<Duration>) -> Self {
        Self {
            accumulator: BatchAccumulator::new(batch_size, batch_flush_interval),
            batch_flush_interval,
            name: String::new(),
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    /// Transforms an input stream into a stream of merged, re-batched RecordBatches.
    ///
    /// Each output batch contains up to `batch_size` rows (merged via `concat_batches`
    /// when multiple small batches are accumulated). When `batch_flush_interval` is set,
    /// partial batches are emitted after the interval. When it is `None`, only
    /// size-based flushing is used.
    pub fn process_stream<S>(
        mut self,
        mut stream: S,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>>
    where
        S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin + 'static,
    {
        async_stream::stream! {
            // Helper macro: merge batches, yield Ok on success, yield Err and return on failure
            macro_rules! yield_merged {
                ($batches:expr) => {
                    match merge_batches($batches) {
                        Ok(Some(merged)) => yield Ok(merged),
                        Ok(None) => {}
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                };
            }

            // Macro for processing a single batch from the input stream
            macro_rules! handle_batch {
                ($batch:expr) => {
                    if let Some(batches) = self.accumulator.push($batch) {
                        yield_merged!(batches);
                        while self.accumulator.should_flush_by_size() {
                            if let Some(batches) = self.accumulator.flush() {
                                yield_merged!(batches);
                            } else {
                                break;
                            }
                        }
                    }
                };
            }

            if let Some(interval) = self.batch_flush_interval {
                // Time + size based flushing
                let mut timer = tokio::time::interval(interval);
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        batch_opt = stream.next() => {
                            match batch_opt {
                                Some(Ok(batch)) => { handle_batch!(batch); }
                                Some(Err(e)) => {
                                    tracing::debug!("BatchAccumulator [{}]: Error from input stream: {}", self.name, e);
                                    yield Err(e);
                                    return;
                                }
                                None => {
                                    if let Some(batches) = self.accumulator.flush_all() {
                                        yield_merged!(batches);
                                    }
                                    break;
                                }
                            }
                        }
                        _ = timer.tick() => {
                            if self.accumulator.should_flush_by_time()
                                && let Some(batches) = self.accumulator.flush() {
                                yield_merged!(batches);
                            }
                        }
                    }
                }
            } else {
                // Size-only flushing (no timer)
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(batch) => { handle_batch!(batch); }
                        Err(e) => {
                            tracing::debug!("BatchAccumulator [{}]: Error from input stream: {}", self.name, e);
                            yield Err(e);
                            return;
                        }
                    }
                }
                // Flush remaining
                if let Some(batches) = self.accumulator.flush_all() {
                    yield_merged!(batches);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::stream;
    use std::sync::Arc;
    use tokio::time::{sleep, timeout};

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();

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

    #[tokio::test]
    async fn test_batch_size_threshold() {
        let accumulator = AsyncBatchAccumulator::new(100, Some(Duration::from_secs(1)));

        let batches = vec![
            Ok(create_test_batch(50)),
            Ok(create_test_batch(30)),
            Ok(create_test_batch(30)),
        ];
        let input = stream::iter(batches);

        let output: Vec<RecordBatch> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(output.len(), 2); // First flush at 100 rows, second flush for remaining 10
        assert_eq!(output[0].num_rows(), 100); // First flush: 50 + 30 + 20 = 100
        assert_eq!(output[1].num_rows(), 10); // Second flush: remaining 10 rows
    }

    #[test]
    fn test_partial_batch_splitting() {
        let mut accumulator = BatchAccumulator::new(75, Some(Duration::from_secs(60)));

        let batch1 = create_test_batch(50);
        assert!(accumulator.push(batch1).is_none());

        let batch2 = create_test_batch(50);
        let result = accumulator.push(batch2);
        assert!(result.is_some());

        let flushed = result.unwrap();
        let total_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 75);
        assert_eq!(accumulator.current_row_count(), 25);
    }

    #[test]
    fn test_manual_time_check() {
        let mut accumulator = BatchAccumulator::new(1000, Some(Duration::from_millis(50)));

        let batch1 = create_test_batch(10);
        assert!(accumulator.push(batch1).is_none());

        assert!(accumulator.check_time_flush().is_none());

        std::thread::sleep(Duration::from_millis(60));

        let result = accumulator.check_time_flush();
        assert!(result.is_some());

        let flushed = result.unwrap();
        let total_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    #[test]
    fn test_flush_all() {
        let mut accumulator = BatchAccumulator::new(1000, Some(Duration::from_secs(60)));

        accumulator.push(create_test_batch(10));
        accumulator.push(create_test_batch(20));
        accumulator.push(create_test_batch(30));

        let result = accumulator.flush_all();
        assert!(result.is_some());

        let flushed = result.unwrap();
        assert_eq!(flushed.len(), 3);
        let total_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 60);
        assert_eq!(accumulator.current_row_count(), 0);
        assert!(accumulator.is_empty());
    }

    #[test]
    fn test_empty_batch_handling() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        let empty_batch = create_test_batch(0);
        // Empty batch without checkpoint data should be discarded
        assert!(accumulator.push(empty_batch).is_none());
        assert_eq!(accumulator.current_row_count(), 0);

        let normal_batch = create_test_batch(50);
        assert!(accumulator.push(normal_batch).is_none());
        assert_eq!(accumulator.current_row_count(), 50);
    }

    #[test]
    fn test_empty_batch_with_checkpoint_metadata_preserved() {
        use crate::checkpoints::checkpoint_management::{
            CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        };
        use crate::utils::batch::enrich_batch_with_metadata;
        use std::collections::HashMap;

        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Create an empty batch with checkpoint metadata
        let empty_batch = create_test_batch(0);
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 1000,
            }],
        );
        let empty_batch_with_checkpoint =
            enrich_batch_with_metadata(empty_batch, metadata).unwrap();

        // Empty batch with checkpoint data should be added to accumulated batches
        // (not returned immediately) to ensure checkpoint is only acknowledged after data is written
        let result = accumulator.push(empty_batch_with_checkpoint);
        assert!(
            result.is_none(),
            "Empty batch with checkpoint data should be added to accumulated batches"
        );

        // Verify it was added to accumulated batches
        assert_eq!(accumulator.accumulated_batches.len(), 1);

        // Flush to get the checkpoint batch
        let flushed = accumulator.flush();
        assert!(flushed.is_some(), "Should flush the checkpoint batch");
        let batches = flushed.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);

        // Verify checkpoint metadata is preserved
        let checkpoint_messages = extract_checkpoint_messages(batches[0].schema().metadata());
        assert_eq!(checkpoint_messages.len(), 1);
        assert!(matches!(
            &checkpoint_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                ..
            }
        ));

        // Row count should not be incremented for empty batches
        assert_eq!(accumulator.current_row_count(), 0);
    }

    #[test]
    fn test_empty_batch_with_checkpoint_added_to_accumulated_batches() {
        use crate::checkpoints::checkpoint_management::{
            CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        };
        use crate::utils::batch::enrich_batch_with_metadata;
        use std::collections::HashMap;

        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Add some accumulated batches
        let batch1 = create_test_batch(50);
        assert!(accumulator.push(batch1).is_none());
        assert_eq!(accumulator.current_row_count(), 50);

        let batch2 = create_test_batch(30);
        assert!(accumulator.push(batch2).is_none());
        assert_eq!(accumulator.current_row_count(), 80);

        // Create an empty batch with checkpoint metadata
        let empty_batch = create_test_batch(0);
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 1000,
            }],
        );
        let empty_batch_with_checkpoint =
            enrich_batch_with_metadata(empty_batch, metadata).unwrap();

        // Empty batch with checkpoint should be added to accumulated batches, not returned immediately
        // This ensures checkpoint is only acknowledged after all prior data is written
        let result = accumulator.push(empty_batch_with_checkpoint);
        assert!(
            result.is_none(),
            "Empty batch with checkpoint data should be added to accumulated batches, not returned immediately"
        );

        // Row count should not change (empty batch doesn't increment count)
        assert_eq!(accumulator.current_row_count(), 80);

        // Accumulator should have 3 batches now: 2 data batches + 1 checkpoint batch
        assert_eq!(accumulator.accumulated_batches.len(), 3);

        // Now flush - checkpoint batch should be included with the data batches
        let flushed = accumulator.flush();
        assert!(
            flushed.is_some(),
            "Should flush accumulated batches including checkpoint"
        );
        let batches = flushed.unwrap();

        // Should have all 3 batches: 2 data batches + 1 checkpoint batch
        assert_eq!(
            batches.len(),
            3,
            "Should flush all accumulated batches including checkpoint batch"
        );
        assert_eq!(batches[0].num_rows(), 50);
        assert_eq!(batches[1].num_rows(), 30);
        assert_eq!(batches[2].num_rows(), 0);

        // Verify checkpoint metadata is on the last batch
        let checkpoint_messages = extract_checkpoint_messages(batches[2].schema().metadata());
        assert_eq!(checkpoint_messages.len(), 1);
        assert!(matches!(
            &checkpoint_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                ..
            }
        ));

        // Accumulator should be empty after flush
        assert_eq!(accumulator.current_row_count(), 0);
        assert!(accumulator.is_empty());
    }

    #[tokio::test]
    async fn test_async_accumulator_flush_remaining_after_completion() {
        let accumulator = AsyncBatchAccumulator::new(100, Some(Duration::from_millis(50)));

        let batches = vec![Ok(create_test_batch(30)), Ok(create_test_batch(20))];
        let input = stream::iter(batches);

        let output: Vec<RecordBatch> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert!(!output.is_empty());
        assert_eq!(total_rows, 50);
    }

    #[tokio::test]
    async fn test_async_accumulator_size_trigger() {
        let accumulator = AsyncBatchAccumulator::new(75, Some(Duration::from_secs(60)));

        let batches = vec![
            Ok(create_test_batch(50)),
            Ok(create_test_batch(50)),
            Ok(create_test_batch(25)),
        ];
        let input = stream::iter(batches);

        let output: Vec<RecordBatch> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].num_rows(), 75);
        assert_eq!(output[1].num_rows(), 50);
    }

    #[tokio::test]
    async fn test_async_accumulator_time_flush() {
        let flush_interval = Duration::from_millis(10);
        let accumulator = AsyncBatchAccumulator::new(1000, Some(flush_interval));

        let input = Box::pin(stream::unfold(0, |state| async move {
            match state {
                0 => {
                    // First batch immediately
                    Some((Ok(create_test_batch(20)), 1))
                }
                1 => {
                    // Wait to trigger time-based flush
                    sleep(Duration::from_millis(15)).await;
                    Some((Ok(create_test_batch(0)), 2))
                }
                2 => {
                    // Another wait
                    sleep(Duration::from_millis(15)).await;
                    Some((Ok(create_test_batch(15)), 3))
                }
                _ => None,
            }
        }));

        let output: Vec<RecordBatch> = timeout(
            Duration::from_secs(2),
            accumulator.process_stream(input).collect::<Vec<_>>(),
        )
        .await
        .expect("timed out")
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 35);
        assert!(
            output.len() >= 2,
            "Expected at least 2 output batches, got {}",
            output.len()
        );
    }

    #[tokio::test]
    async fn test_async_accumulator_time_flush_cant_exceed_batch_size_on_remainder() {
        let batch_size = 1_000usize;
        let flush_interval = Duration::from_millis(50);
        let accumulator = AsyncBatchAccumulator::new(batch_size, Some(flush_interval));

        let input = Box::pin(stream::unfold(0, |state| async move {
            match state {
                0 => {
                    // One big batch that exceeds batch_size by 1,500
                    Some((Ok(create_test_batch(2_500)), 1))
                }
                1 => {
                    sleep(Duration::from_millis(150)).await;
                    None
                }
                _ => None,
            }
        }));

        let output: Vec<RecordBatch> = timeout(
            Duration::from_secs(2),
            accumulator.process_stream(input).collect::<Vec<_>>(),
        )
        .await
        .expect("timed out")
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

        assert_eq!(
            output[0].num_rows(),
            batch_size,
            "First flush should equal batch_size"
        );
        assert_eq!(
            output[1].num_rows(),
            batch_size,
            "Second flush should equal batch_size"
        );
        assert_eq!(output[2].num_rows(), 500, "Third flush should be remainder");
    }

    #[test]
    fn test_batch_split_data_loss() {
        // Tests if, when a batch is split, the remaining data in the batch is not lost.
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        accumulator
            .accumulated_batches
            .push_back(create_test_batch(30)); // First batch
        accumulator
            .accumulated_batches
            .push_back(create_test_batch(80)); // Large batch that will be split
        accumulator
            .accumulated_batches
            .push_back(create_test_batch(25)); // This batch was lost in a previous bug
        accumulator.current_row_count = 135; // Total rows

        let result = accumulator.flush();
        assert!(result.is_some());

        let flushed = result.unwrap();
        let flushed_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();

        // Get the remaining data to verify total
        let remaining_data = accumulator.flush_all();
        let remaining_rows = if let Some(batches) = remaining_data {
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        } else {
            0
        };

        // Expected: 100 flushed + 35 remaining = 135 total
        assert_eq!(flushed_rows + remaining_rows, 135);
    }

    #[tokio::test]
    async fn test_async_accumulator_continues_flushing_when_batch_exceeds_size() {
        // Test that when a batch comes in that exceeds batch_size, it continues flushing
        // until all rows are processed (even within a single push)
        let accumulator = AsyncBatchAccumulator::new(5, Some(Duration::from_secs(60)));

        // Send a single batch of 13 rows - with batch_size=5, this should flush 5, 5, 3
        let input = stream::iter(vec![Ok(create_test_batch(13))]);

        let output: Vec<RecordBatch> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 13, "All 13 rows should be processed");
        assert_eq!(output.len(), 3, "Should flush 3 times: 5, 5, 3");
        assert_eq!(output[0].num_rows(), 5);
        assert_eq!(output[1].num_rows(), 5);
        assert_eq!(output[2].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_async_accumulator_processes_all_records_with_batch_size_one() {
        // Test the specific scenario from the bug: batch_size=1, single batch with multiple rows
        let accumulator = AsyncBatchAccumulator::new(1, Some(Duration::from_secs(60)));

        let input = stream::iter(vec![Ok(create_test_batch(3))]);

        let output: Vec<RecordBatch> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "All 3 rows should be processed");
        assert_eq!(
            output.len(),
            3,
            "Should flush 3 times (once per row with batch_size=1)"
        );
    }

    #[tokio::test]
    async fn test_async_accumulator_empty_stream_produces_no_output() {
        let accumulator = AsyncBatchAccumulator::new(1000, Some(Duration::from_millis(20)));

        let empty_batches: Vec<Result<RecordBatch, DataFusionError>> = vec![];
        let input = stream::iter(empty_batches);

        let output: Vec<Result<RecordBatch, DataFusionError>> = accumulator
            .process_stream(Box::pin(input))
            .collect::<Vec<_>>()
            .await;

        assert!(
            output.is_empty(),
            "Empty input stream should produce no output"
        );
    }

    #[tokio::test]
    async fn test_async_accumulator_flushes_on_timer_when_idle() {
        // Verify that accumulated data is flushed on timer tick even when no new
        // batches arrive. This is important for checkpoint-only batches during
        // idle periods.
        let flush_interval = Duration::from_millis(25);
        let accumulator = AsyncBatchAccumulator::new(1000, Some(flush_interval));

        // Stream that sends one small batch, then goes idle for a long time
        let input = Box::pin(stream::unfold(0, |state| async move {
            match state {
                0 => Some((Ok(create_test_batch(5)), 1)),
                _ => {
                    sleep(Duration::from_secs(10)).await;
                    None
                }
            }
        }));

        let mut output_stream = Box::pin(accumulator.process_stream(input));

        // The 5-row batch should be flushed by the timer within 200ms,
        // not stuck waiting for batch_size=1000
        let first_batch = timeout(Duration::from_millis(200), output_stream.next())
            .await
            .expect("Timer should flush the accumulated batch within 200ms")
            .expect("Stream should produce at least one batch")
            .expect("Batch should not be an error");

        assert_eq!(first_batch.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_async_accumulator_flushes_checkpoint_only_batches_on_timer() {
        // Verify that checkpoint-only (empty) batches are flushed on timer tick.
        // This ensures checkpoint acknowledgments are not delayed during idle periods.
        use crate::checkpoints::checkpoint_management::{
            CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        };
        use crate::utils::batch::enrich_batch_with_metadata;

        let flush_interval = Duration::from_millis(25);
        let accumulator = AsyncBatchAccumulator::new(1000, Some(flush_interval));

        // Create a checkpoint-only batch (0 rows, with checkpoint metadata)
        let empty = create_test_batch(0);
        let mut metadata = std::collections::HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 1000,
            }],
        );
        let checkpoint_batch = enrich_batch_with_metadata(empty, metadata).unwrap();

        // Stream that sends the checkpoint batch, then goes idle
        let input = Box::pin(stream::unfold(Some(checkpoint_batch), |state| async move {
            match state {
                Some(batch) => Some((Ok(batch), None)),
                None => {
                    sleep(Duration::from_secs(10)).await;
                    None
                }
            }
        }));

        let mut output_stream = Box::pin(accumulator.process_stream(input));

        // The checkpoint batch should be flushed by the timer within 200ms
        let first_batch = timeout(Duration::from_millis(200), output_stream.next())
            .await
            .expect("Timer should flush checkpoint batch within 200ms")
            .expect("Stream should produce at least one batch")
            .expect("Batch should not be an error");

        let checkpoint_messages = extract_checkpoint_messages(first_batch.schema().metadata());
        assert_eq!(
            checkpoint_messages.len(),
            1,
            "Checkpoint metadata should be preserved"
        );
    }

    fn create_test_batch_with_checkpoint(num_rows: usize, epoch: u64) -> RecordBatch {
        use crate::checkpoints::checkpoint_management::{
            CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        };
        use crate::utils::batch::enrich_batch_with_metadata;
        use std::collections::HashMap;

        let batch = create_test_batch(num_rows);
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(epoch),
                created_at_ms: 1000,
            }],
        );
        enrich_batch_with_metadata(batch, metadata).unwrap()
    }

    #[test]
    fn test_slice_does_not_duplicate_checkpoint_metadata() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Push a 150-row batch with checkpoint marker
        let batch = create_test_batch_with_checkpoint(150, 1);
        let result = accumulator.push(batch);

        // First flush should yield batches with the marker (first 100 rows)
        assert!(result.is_some());
        let flushed = result.unwrap();
        let total_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);

        // Verify first flush has checkpoint metadata
        let has_checkpoint = flushed
            .iter()
            .any(|b| !extract_checkpoint_messages(b.schema().metadata()).is_empty());
        assert!(
            has_checkpoint,
            "First flush should have checkpoint metadata"
        );

        // Second flush (remainder 50 rows) should have NO checkpoint metadata
        let result2 = accumulator.flush();
        assert!(result2.is_some());
        let flushed2 = result2.unwrap();
        let total_rows2: usize = flushed2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows2, 50);

        let has_checkpoint2 = flushed2
            .iter()
            .any(|b| !extract_checkpoint_messages(b.schema().metadata()).is_empty());
        assert!(
            !has_checkpoint2,
            "Second flush (remainder) should NOT have checkpoint metadata"
        );
    }

    #[test]
    fn test_large_batch_produces_single_checkpoint_ack() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Push a 500-row batch with checkpoint marker
        let batch = create_test_batch_with_checkpoint(500, 1);

        // Count checkpoint messages from all returned batches (push + flush cycles)
        let mut total_checkpoint_messages = 0;

        // push() triggers the first flush when batch_size is exceeded
        if let Some(batches) = accumulator.push(batch) {
            for b in &batches {
                total_checkpoint_messages +=
                    extract_checkpoint_messages(b.schema().metadata()).len();
            }
        }

        // Continue flushing remaining rows
        while let Some(batches) = accumulator.flush() {
            for b in &batches {
                total_checkpoint_messages +=
                    extract_checkpoint_messages(b.schema().metadata()).len();
            }
        }

        assert_eq!(
            total_checkpoint_messages, 1,
            "Should have exactly 1 checkpoint message across all flush cycles, got {}",
            total_checkpoint_messages
        );
    }

    #[test]
    fn test_checkpoint_metadata_preserved_when_previous_batches_exactly_fill_capacity() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        assert!(accumulator.push(create_test_batch(40)).is_none());
        assert!(accumulator.push(create_test_batch(60)).is_some());

        let checkpoint_batch = create_test_batch_with_checkpoint(10, 7);
        assert!(accumulator.push(checkpoint_batch).is_none());

        let flushed = accumulator.flush().expect("checkpoint batch should flush");
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].num_rows(), 10);

        let checkpoint_messages = extract_checkpoint_messages(flushed[0].schema().metadata());
        assert_eq!(checkpoint_messages.len(), 1);
        assert!(matches!(
            &checkpoint_messages[0],
            crate::checkpoints::checkpoint_management::CheckpointMessage::Marker {
                epoch: crate::checkpoints::checkpoint_management::CheckpointEpoch(7),
                ..
            }
        ));
    }

    #[test]
    fn test_multiple_batches_with_different_epochs_preserved() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Push two batches with different epoch markers
        let batch1 = create_test_batch_with_checkpoint(50, 1);
        let batch2 = create_test_batch_with_checkpoint(50, 2);

        // Collect checkpoint messages from all returned batches
        let mut all_epochs: Vec<u64> = Vec::new();

        let count_epochs = |batches: &VecDeque<RecordBatch>, epochs: &mut Vec<u64>| {
            for b in batches {
                for msg in extract_checkpoint_messages(b.schema().metadata()) {
                    if let crate::checkpoints::checkpoint_management::CheckpointMessage::Marker {
                        epoch,
                        ..
                    } = msg
                    {
                        epochs.push(epoch.0);
                    }
                }
            }
        };

        if let Some(batches) = accumulator.push(batch1) {
            count_epochs(&batches, &mut all_epochs);
        }
        if let Some(batches) = accumulator.push(batch2) {
            count_epochs(&batches, &mut all_epochs);
        }

        // Flush remaining
        while let Some(batches) = accumulator.flush() {
            count_epochs(&batches, &mut all_epochs);
        }

        assert_eq!(
            all_epochs.len(),
            2,
            "Should have exactly 2 checkpoint messages, got {:?}",
            all_epochs
        );
        assert!(all_epochs.contains(&1), "Should contain epoch 1");
        assert!(all_epochs.contains(&2), "Should contain epoch 2");
    }

    // ==================== merge_batches tests ====================

    #[test]
    fn test_merge_batches_empty_input() {
        let result = merge_batches(VecDeque::new()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_batches_single_batch_no_copy() {
        let batch = create_test_batch(50);
        let mut batches = VecDeque::new();
        batches.push_back(batch);

        let merged = merge_batches(batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 50);
    }

    #[test]
    fn test_merge_batches_multiple_data_batches() {
        let mut batches = VecDeque::new();
        batches.push_back(create_test_batch(30));
        batches.push_back(create_test_batch(20));
        batches.push_back(create_test_batch(50));

        let merged = merge_batches(batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 100);
        assert_eq!(merged.num_columns(), 2);
    }

    #[test]
    fn test_merge_batches_preserves_checkpoint_metadata() {
        let mut batches = VecDeque::new();
        batches.push_back(create_test_batch_with_checkpoint(30, 1));
        batches.push_back(create_test_batch(20));
        batches.push_back(create_test_batch_with_checkpoint(10, 2));

        let merged = merge_batches(batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 60);

        let messages = extract_checkpoint_messages(merged.schema().metadata());
        assert_eq!(
            messages.len(),
            2,
            "Both checkpoint messages should be preserved"
        );
    }

    #[test]
    fn test_merge_batches_checkpoint_only_batches() {
        use crate::checkpoints::checkpoint_management::{CheckpointEpoch, CheckpointMessage};

        let mut batches = VecDeque::new();
        // Create empty batch with checkpoint
        let empty = create_test_batch(0);
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(5),
                created_at_ms: 1000,
            }],
        );
        let checkpoint_batch = enrich_batch_with_metadata(empty, metadata).unwrap();
        batches.push_back(checkpoint_batch);

        let merged = merge_batches(batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 0);

        let messages = extract_checkpoint_messages(merged.schema().metadata());
        assert_eq!(messages.len(), 1);
    }

    #[test]
    fn test_merge_batches_mixed_data_and_checkpoint_only() {
        use crate::checkpoints::checkpoint_management::{CheckpointEpoch, CheckpointMessage};

        let mut batches = VecDeque::new();
        batches.push_back(create_test_batch(40));

        // Add empty checkpoint-only batch
        let empty = create_test_batch(0);
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(3),
                created_at_ms: 1000,
            }],
        );
        let checkpoint_batch = enrich_batch_with_metadata(empty, metadata).unwrap();
        batches.push_back(checkpoint_batch);

        batches.push_back(create_test_batch(10));

        let merged = merge_batches(batches).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 50);

        let messages = extract_checkpoint_messages(merged.schema().metadata());
        assert_eq!(
            messages.len(),
            1,
            "Checkpoint from empty batch should be preserved"
        );
    }

    #[test]
    fn test_flush_merged_returns_single_batch() {
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Push batches that exceed batch_size. The third push triggers flush via push(),
        // so we capture that result to verify merge works.
        assert!(accumulator.push(create_test_batch(30)).is_none());
        assert!(accumulator.push(create_test_batch(40)).is_none());
        let flushed = accumulator.push(create_test_batch(40));
        assert!(flushed.is_some());

        let merged = merge_batches(flushed.unwrap()).unwrap().unwrap();
        assert_eq!(merged.num_rows(), 100);
    }

    #[test]
    fn test_flush_all_merged_returns_single_batch() {
        let mut accumulator = BatchAccumulator::new(1000, Some(Duration::from_secs(60)));

        accumulator.push(create_test_batch(10));
        accumulator.push(create_test_batch(20));
        accumulator.push(create_test_batch(30));

        let merged = accumulator.flush_all_merged().unwrap().unwrap();
        assert_eq!(merged.num_rows(), 60);
    }

    /// Create a test batch with a configurable string column that has predictable byte size.
    /// Each row's string value is `content` repeated `repeats` times.
    fn create_test_batch_with_string_size(
        num_rows: usize,
        content: &str,
        repeats: usize,
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]);

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let value = content.repeat(repeats);
        let names: Vec<&str> = (0..num_rows).map(|_| value.as_str()).collect();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_byte_size_flush_triggers_before_row_limit() {
        // Create accumulator with high row limit but low byte limit
        // Row limit: 10000, byte limit: 1000 bytes
        let mut accumulator = BatchAccumulator::with_max_string_bytes(
            10000,
            Some(Duration::from_secs(60)),
            Some(1000),
        );

        // Create a batch with 10 rows, each having 100 bytes of string data
        // Total: 10 * 100 = 1000 bytes (at limit)
        let batch1 = create_test_batch_with_string_size(10, "x", 100);
        // Should not flush yet (at limit, not over)
        assert!(accumulator.push(batch1).is_none());
        assert!(accumulator.should_flush_by_size()); // Should be at the threshold

        // Add one more byte and it should flush
        let batch2 = create_test_batch_with_string_size(1, "y", 1);
        let result = accumulator.push(batch2);

        // Should have flushed due to byte limit
        assert!(result.is_some(), "Should flush when byte limit is exceeded");
        let flushed = result.unwrap();
        let total_rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 11); // All rows from both batches
    }

    #[test]
    fn test_byte_size_not_checked_when_none() {
        // Create accumulator without byte limit
        let mut accumulator = BatchAccumulator::new(100, Some(Duration::from_secs(60)));

        // Create large batch that would exceed any reasonable byte limit
        let batch = create_test_batch_with_string_size(50, "x", 10000); // 50 * 10000 = 500KB
        assert!(accumulator.push(batch).is_none()); // Should not flush (only 50 rows < 100)

        // Only row count matters
        let batch2 = create_test_batch_with_string_size(50, "x", 10000);
        let result = accumulator.push(batch2);
        assert!(result.is_some()); // Now flushes because row count hit 100
    }

    #[test]
    fn test_accumulated_string_bytes_reset_on_flush_all() {
        let mut accumulator = BatchAccumulator::with_max_string_bytes(
            10000,
            Some(Duration::from_secs(60)),
            Some(1000),
        );

        let batch = create_test_batch_with_string_size(10, "x", 50); // ~500 bytes
        accumulator.push(batch);

        // After flush_all, accumulated_string_bytes should be reset
        let _ = accumulator.flush_all();
        assert!(accumulator.is_empty());

        // Push another batch - should not trigger flush since we're starting fresh
        let batch2 = create_test_batch_with_string_size(5, "x", 50); // ~250 bytes
        assert!(accumulator.push(batch2).is_none());
    }

    #[test]
    fn test_default_max_string_bytes_constant() {
        // Verify the constant is approximately 1 GiB (half of i32::MAX)
        assert_eq!(DEFAULT_MAX_STRING_BYTES, (i32::MAX as usize) / 2);
        // Should be approximately 1 GiB
        assert!(DEFAULT_MAX_STRING_BYTES > 1_000_000_000); // > 1 GB
        assert!(DEFAULT_MAX_STRING_BYTES < 2_000_000_000); // < 2 GB
    }
}
