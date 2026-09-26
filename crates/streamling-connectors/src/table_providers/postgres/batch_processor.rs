use arrow_schema::SchemaRef;
use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use streamling_core::checkpoints::checkpoint_management::{
    CheckpointMessage, extract_checkpoint_messages, now_ms, report_marker_at_sink,
    send_checkpoint_ack,
};
use streamling_core::data::{COLUMN_NAME_OP, RowKind};
use streamling_core::streamling_err;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::MetricsRecorder;
use streamling_core::utils::arrow::{build_schema_from_columns, safe_take};

use crate::table_providers::postgres::execution::{
    SinkContext, execute_batch_delete, execute_batch_insert,
};
use crate::table_providers::postgres::query_builder::PostgresQueryBuilder;
use streamling_core::utils::pg::truncate_finalized_checkpoint_data;

/// Filter a RecordBatch to only include rows at the specified indices
/// Uses safe_take to handle potential overflow with large string/binary arrays
fn filter_batch_by_indices(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    // Convert indices to Int64Array for safe_take
    let indices_i64: Vec<i64> = indices.iter().map(|&i| i as i64).collect();
    let indices_array = Int64Array::from(indices_i64);

    let filtered_columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .map(|col| safe_take(col, &indices_array))
        .collect::<Result<Vec<_>>>()?;

    // Build schema from actual column types to handle type promotions (e.g., Utf8 -> LargeUtf8)
    let new_schema = build_schema_from_columns(
        &batch.schema(),
        &filtered_columns,
        batch.schema().metadata().clone(),
    );

    RecordBatch::try_new(new_schema, filtered_columns)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
}

/// Context for batch processing
#[derive(Clone)]
pub struct BatchProcessorContext {
    pub pool: sqlx::PgPool,
    pub schema_name: String,
    pub table: String,
    pub primary_key_columns: Vec<String>,
    pub primary_key_indices: Vec<usize>,
    pub column_names: Vec<String>,
    pub column_indices: Vec<usize>,
    pub node_label: String,
    pub records_processed: Arc<Mutex<u64>>,
    pub num_records_before_stop: Option<u64>,
    pub metrics_recorder: Arc<MetricsRecorder>,
    pub metric_metadata_id: String,
    pub original_schema: SchemaRef,
    pub on_conflict: String,
    pub update_where: Option<BTreeMap<String, String>>,
    pub append_only_mode: bool,
    pub checkpoint_truncation: bool,
    pub current_checkpoint_epoch: Arc<Mutex<u64>>,
    /// Client-side bound for a single statement execution; see
    /// `PostgresSinkConfig::client_statement_timeout`.
    pub client_statement_timeout: Option<Duration>,
}

/// Handle checkpoint truncation for finalized epochs
async fn handle_checkpoint_truncation(context: &BatchProcessorContext, truncate_epoch: u64) {
    tracing::debug!(
        "Handling checkpoint truncation for epoch {} on table {}.{}",
        truncate_epoch,
        context.schema_name,
        context.table
    );

    // Truncate finalized data (best-effort, errors are logged but don't fail pipeline)
    let _ = truncate_finalized_checkpoint_data(
        &context.pool,
        &context.schema_name,
        &context.table,
        truncate_epoch,
        context.client_statement_timeout,
    )
    .await;
}

/// Process checkpoint messages with metrics and handle Finalizer for truncation.
/// This is a PostgreSQL-specific version that extends the standard process_checkpoint_acks
/// to handle Finalizer messages for checkpoint truncation.
async fn process_checkpoint_messages_with_truncation(
    messages: Vec<CheckpointMessage>,
    arrival_time_ms: u64,
    ack_start: Instant,
    context: &BatchProcessorContext,
) {
    for message in messages {
        match message {
            CheckpointMessage::Marker {
                epoch,
                created_at_ms,
            } => {
                // Record marker arrival time
                let arrival_latency_ms = arrival_time_ms.saturating_sub(created_at_ms);
                context.metrics_recorder.record_time(
                    "checkpoint_marker_arrival",
                    Duration::from_millis(arrival_latency_ms),
                    &context.metric_metadata_id,
                );

                let sink_id = get_reference_name_from_metric_key(&context.metric_metadata_id);
                // Best-effort: during shutdown a source may have dropped its
                // channel receiver already; `send_checkpoint_ack` logs instead
                // of panicking so a failed broadcast can't kill the sink
                // mid-drain (the epoch simply never finalizes).
                if report_marker_at_sink(&sink_id, epoch.clone()) {
                    send_checkpoint_ack(epoch, &sink_id);
                }

                // Record sink flush time (time from batch arrival to ack sent)
                context.metrics_recorder.record_time(
                    "checkpoint_sink_flush",
                    ack_start.elapsed(),
                    &context.metric_metadata_id,
                );
            }
            CheckpointMessage::Finalizer(epoch) => {
                // Handle truncation for finalized epochs
                // NOTE: this is currently a BLOCKING operation. We can consider making it run concurrently
                if context.checkpoint_truncation && epoch.0 > 0 {
                    let truncate_epoch = epoch.0 - 1; // Truncate N-1 when Finalizer(N) received
                    handle_checkpoint_truncation(context, truncate_epoch).await;
                }
            }
            _ => {}
        }
    }
}

/// Execute an INSERT/UPSERT for a single slice of a RecordBatch
async fn execute_insert_slice(
    context: &BatchProcessorContext,
    slice: &RecordBatch,
    epoch_for_rows: Option<u64>,
) -> streamling_core::error::Result<()> {
    if slice.num_rows() == 0 {
        return Ok(());
    }

    let query = PostgresQueryBuilder::build_complete_upsert_query(
        &context.schema_name,
        &context.table,
        &context.column_names,
        &context.primary_key_columns,
        slice.num_rows(),
        Some(&context.original_schema),
        &context.on_conflict,
        context.update_where.as_ref(),
        &context.table,
        epoch_for_rows,
    );

    let columns: Vec<_> = (0..slice.num_columns())
        .map(|i| slice.column(i).clone())
        .collect();

    let sink_ctx = SinkContext {
        sink_name: context.node_label.clone(),
        schema_name: context.schema_name.clone(),
        table_name: context.table.clone(),
    };

    execute_batch_insert(
        &context.pool,
        &query,
        &columns,
        &context.column_indices,
        slice.num_rows(),
        &sink_ctx,
        epoch_for_rows,
        context.client_statement_timeout,
    )
    .await
}

/// Execute a DELETE for a single slice of a RecordBatch
async fn execute_delete_slice(
    context: &BatchProcessorContext,
    slice: &RecordBatch,
) -> streamling_core::error::Result<()> {
    if slice.num_rows() == 0 {
        return Ok(());
    }

    let query = PostgresQueryBuilder::build_delete_query(
        &context.schema_name,
        &context.table,
        &context.primary_key_columns,
        slice.num_rows(),
    );

    let columns: Vec<_> = (0..slice.num_columns())
        .map(|i| slice.column(i).clone())
        .collect();

    let sink_ctx = SinkContext {
        sink_name: context.node_label.clone(),
        schema_name: context.schema_name.clone(),
        table_name: context.table.clone(),
    };

    execute_batch_delete(
        &context.pool,
        &query,
        &columns,
        &context.primary_key_indices,
        slice.num_rows(),
        &sink_ctx,
        context.client_statement_timeout,
    )
    .await
}

/// Process a single batch and return whether to continue processing.
/// Deduplication and batch accumulation are handled upstream by WrappingDataSink.
pub async fn process_batch(context: &BatchProcessorContext, batch: RecordBatch) -> Result<bool> {
    // Capture arrival time at the start for accurate marker arrival latency
    let arrival_time_ms = now_ms();
    // Capture ack_start before DB writes so checkpoint_sink_flush includes write time
    let ack_start = Instant::now();
    let start_at = Instant::now();

    let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
    tracing::trace!(
        "Extracted checkpoint messages from batch metadata: {:?}",
        checkpoint_messages
    );

    // Update current epoch from the last Marker message if checkpoint truncation is enabled.
    if context.checkpoint_truncation {
        for message in &checkpoint_messages {
            if let CheckpointMessage::Marker { epoch, .. } = message {
                let mut current_epoch = context.current_checkpoint_epoch.lock().unwrap();
                *current_epoch = epoch.0;
                tracing::debug!("Updated current checkpoint epoch to: {}", epoch.0);
            }
        }
    }

    // Handle empty batches (may carry checkpoint metadata)
    if batch.num_rows() == 0 {
        process_checkpoint_messages_with_truncation(
            checkpoint_messages,
            arrival_time_ms,
            ack_start,
            context,
        )
        .await;
        return Ok(true);
    }

    // Get current checkpoint epoch if checkpoint truncation is enabled
    let epoch_for_rows = if context.checkpoint_truncation {
        Some(*context.current_checkpoint_epoch.lock().unwrap())
    } else {
        None
    };

    if context.append_only_mode {
        // Append-only mode: INSERT everything (including deletes with _gs_op='d')
        // Don't split by operation type, no DELETE statements
        execute_insert_slice(context, &batch, epoch_for_rows).await?;
    } else {
        // Normal mode: Split by _gs_op and process inserts/updates vs deletes separately
        // Get _gs_op column to identify delete operations
        let op_column = batch
            .column_by_name(COLUMN_NAME_OP)
            .ok_or_else(|| streamling_err!("missing required column: {}", COLUMN_NAME_OP))?;
        let op_array = op_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| streamling_err!("column {} must be StringArray", COLUMN_NAME_OP))?;

        // Split rows into inserts/updates and deletes
        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for (idx, op) in op_array.iter().enumerate() {
            if let Some(op_str) = op {
                let row_kind = RowKind::from_str(op_str).unwrap_or(RowKind::Insert);
                match row_kind {
                    RowKind::Delete => delete_indices.push(idx),
                    RowKind::Insert | RowKind::Update => insert_indices.push(idx),
                }
            } else {
                // Default to insert if op is null
                insert_indices.push(idx);
            }
        }

        // Process inserts/updates
        if !insert_indices.is_empty() {
            let filtered_batch = filter_batch_by_indices(&batch, &insert_indices)?;

            execute_insert_slice(context, &filtered_batch, epoch_for_rows).await?;
        }

        // Process deletes
        if !delete_indices.is_empty() && !context.primary_key_columns.is_empty() {
            let delete_batch = filter_batch_by_indices(&batch, &delete_indices)?;

            execute_delete_slice(context, &delete_batch).await?;
        }
    }

    // Process checkpoint messages after successful write
    process_checkpoint_messages_with_truncation(
        checkpoint_messages,
        arrival_time_ms,
        ack_start,
        context,
    )
    .await;

    context
        .metrics_recorder
        .record_output_rows_count(batch.num_rows() as u64, &context.metric_metadata_id);
    context
        .metrics_recorder
        .record_elapsed_compute(start_at.elapsed(), &context.metric_metadata_id);

    tracing::debug!(
        "[{}] wrote batch of {} rows (table '{}.{}')",
        context.node_label,
        batch.num_rows() as u64,
        context.schema_name,
        context.table,
    );

    if let Some(limit) = context.num_records_before_stop {
        let mut count = context.records_processed.lock().unwrap();
        *count += batch.num_rows() as u64;
        let current_count = *count;
        drop(count);
        tracing::info!(
            "[{}] records processed: {} (table '{}.{}')",
            context.node_label,
            current_count,
            context.schema_name,
            context.table,
        );
        if current_count >= limit {
            // Record-limit reached: request process-wide graceful shutdown so
            // every source drains and ends its stream — the same path SIGTERM
            // takes (test-only mode).
            streamling_core::shutdown::request_shutdown();
            Ok(false)
        } else {
            Ok(true)
        }
    } else {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};

    use std::sync::Arc;

    #[tokio::test]
    async fn test_process_batch_empty_batch() {
        // Test that an empty batch returns Ok(true) and processes checkpoint
        use streamling_core::telemetry::recorder::get_metrics_recorder;

        // Create a dummy schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a lazy pool (doesn't require immediate connection)
        let pool = sqlx::PgPool::connect_lazy("postgresql://test:test@localhost/test").unwrap();

        let context = BatchProcessorContext {
            pool,
            schema_name: "test_schema".to_string(),
            table: "test_table".to_string(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_indices: vec![0],
            column_names: vec!["id".to_string(), "name".to_string()],
            column_indices: vec![0, 1],
            node_label: "postgres sink 'test_sink'".to_string(),
            records_processed: Arc::new(Mutex::new(0u64)),
            num_records_before_stop: None,
            metrics_recorder: get_metrics_recorder().clone(),
            metric_metadata_id: "test_metric".to_string(),
            original_schema: schema.clone(),
            on_conflict: "update".to_string(),
            update_where: None,
            append_only_mode: false,
            checkpoint_truncation: false,
            current_checkpoint_epoch: Arc::new(Mutex::new(0)),
            client_statement_timeout: Some(Duration::from_secs(120)),
        };

        let empty_batch = RecordBatch::new_empty(schema);

        // Empty batch should return Ok(true) after processing checkpoint
        // The function returns early before attempting DB operations
        let result = process_batch(&context, empty_batch).await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_batch_processor_context_clone() {
        // Verify that BatchProcessorContext can be cloned (required for async closures)
        use streamling_core::telemetry::recorder::get_metrics_recorder;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let pool = sqlx::PgPool::connect_lazy("postgresql://test:test@localhost/test").unwrap();

        let context = BatchProcessorContext {
            pool,
            schema_name: "test_schema".to_string(),
            table: "test_table".to_string(),
            primary_key_columns: vec![],
            primary_key_indices: vec![],
            column_names: vec!["id".to_string()],
            column_indices: vec![0],
            node_label: "postgres sink 'test_sink'".to_string(),
            records_processed: Arc::new(Mutex::new(0u64)),
            num_records_before_stop: None,
            metrics_recorder: get_metrics_recorder().clone(),
            metric_metadata_id: "test_metric".to_string(),
            original_schema: schema,
            on_conflict: "update".to_string(),
            update_where: None,
            append_only_mode: false,
            checkpoint_truncation: false,
            current_checkpoint_epoch: Arc::new(Mutex::new(0)),
            client_statement_timeout: Some(Duration::from_secs(120)),
        };

        let cloned = context.clone();

        assert_eq!(context.schema_name, cloned.schema_name);
        assert_eq!(context.table, cloned.table);
        assert_eq!(context.column_names, cloned.column_names);
    }

    #[tokio::test]
    async fn test_checkpoint_metadata_preserved_across_multiple_batches() {
        // Test that checkpoint metadata from all batches is preserved through concatenation/deduplication
        use datafusion::arrow::array::{Int64Array, StringArray};
        use std::collections::HashMap;
        use std::sync::Arc;
        use streamling_core::checkpoints::checkpoint_management::{
            CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
            extract_checkpoint_messages,
        };
        use streamling_core::data::COLUMN_NAME_OP;
        use streamling_core::utils::batch::enrich_batch_with_metadata;

        // Create schema with _gs_op column required by batch processor
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));

        // Create first batch with checkpoint metadata (epoch 1)
        let id_array1: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let name_array1: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let op_array1: ArrayRef = Arc::new(StringArray::from(vec!["i", "i"]));
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![id_array1, name_array1, op_array1]).unwrap();

        let mut metadata1 = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata1,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 1000,
            }],
        );
        let batch1_with_checkpoint = enrich_batch_with_metadata(batch1, metadata1).unwrap();

        // Create second batch with checkpoint metadata (epoch 2)
        let id_array2: ArrayRef = Arc::new(Int64Array::from(vec![3, 4]));
        let name_array2: ArrayRef = Arc::new(StringArray::from(vec!["Charlie", "David"]));
        let op_array2: ArrayRef = Arc::new(StringArray::from(vec!["i", "i"]));
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![id_array2, name_array2, op_array2]).unwrap();

        let mut metadata2 = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata2,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(2),
                created_at_ms: 2000,
            }],
        );
        let batch2_with_checkpoint = enrich_batch_with_metadata(batch2, metadata2).unwrap();

        // Create third batch with checkpoint metadata (epoch 3)
        let id_array3: ArrayRef = Arc::new(Int64Array::from(vec![5]));
        let name_array3: ArrayRef = Arc::new(StringArray::from(vec!["Eve"]));
        let op_array3: ArrayRef = Arc::new(StringArray::from(vec!["i"]));
        let batch3 =
            RecordBatch::try_new(schema.clone(), vec![id_array3, name_array3, op_array3]).unwrap();

        let mut metadata3 = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata3,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(3),
                created_at_ms: 3000,
            }],
        );
        let batch3_with_checkpoint = enrich_batch_with_metadata(batch3, metadata3).unwrap();

        // Verify checkpoint metadata exists in all batches before processing
        let checkpoint1 = extract_checkpoint_messages(batch1_with_checkpoint.schema().metadata());
        let checkpoint2 = extract_checkpoint_messages(batch2_with_checkpoint.schema().metadata());
        let checkpoint3 = extract_checkpoint_messages(batch3_with_checkpoint.schema().metadata());

        assert_eq!(checkpoint1.len(), 1);
        assert_eq!(checkpoint2.len(), 1);
        assert_eq!(checkpoint3.len(), 1);
        assert!(matches!(
            &checkpoint1[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                ..
            }
        ));
        assert!(matches!(
            &checkpoint2[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(2),
                ..
            }
        ));
        assert!(matches!(
            &checkpoint3[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(3),
                ..
            }
        ));

        // Simulate what process_batches does: extract checkpoint messages from ALL batches
        // BEFORE concatenation/deduplication (this is the fix)
        let batches_vec = vec![
            batch1_with_checkpoint.clone(),
            batch2_with_checkpoint.clone(),
            batch3_with_checkpoint.clone(),
        ];

        // Extract checkpoint messages from all batches before deduplication
        // This is what the fixed implementation does - it extracts from all batches
        // before Arrow's concat_batches loses metadata from batches other than the first
        let mut all_extracted_checkpoints = Vec::new();
        for batch in &batches_vec {
            all_extracted_checkpoints
                .extend(extract_checkpoint_messages(batch.schema().metadata()));
        }

        // Verify all checkpoint messages are extracted from all batches
        assert_eq!(
            all_extracted_checkpoints.len(),
            3,
            "All checkpoint messages from all batches should be extracted before deduplication"
        );

        // Verify all epochs are present
        let epochs: Vec<u64> = all_extracted_checkpoints
            .iter()
            .filter_map(|msg| {
                if let CheckpointMessage::Marker { epoch, .. } = msg {
                    Some(epoch.0)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(epochs.len(), 3);
        assert!(epochs.contains(&1), "Epoch 1 should be present");
        assert!(epochs.contains(&2), "Epoch 2 should be present");
        assert!(epochs.contains(&3), "Epoch 3 should be present");

        // Verify that after deduplication, metadata is lost (this is expected behavior)
        // This is why WrappingDataSink extracts checkpoint messages BEFORE deduplication
        use streamling_core::utils::dedup::deduplicate_record_batches;
        let primary_key_str = "id";
        let deduplicated = deduplicate_record_batches(&batches_vec, primary_key_str).unwrap();
        let deduplicated_checkpoints =
            extract_checkpoint_messages(deduplicated.schema().metadata());

        // After deduplication, only metadata from the first batch is preserved
        assert_eq!(
            deduplicated_checkpoints.len(),
            1,
            "After deduplication, only metadata from the first batch is preserved (expected Arrow behavior)"
        );
        assert!(matches!(
            &deduplicated_checkpoints[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                ..
            }
        ));
    }

    #[test]
    fn test_filter_batch_by_indices_basic() {
        // Test basic filtering with simple types
        use datafusion::arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ]));

        let batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();

        // Filter to keep rows 0, 2, 4 (Alice, Charlie, Eve)
        let indices = vec![0, 2, 4];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        assert_eq!(filtered.num_rows(), 3);

        let filtered_ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let filtered_names = filtered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(filtered_ids.value(0), 1);
        assert_eq!(filtered_ids.value(1), 3);
        assert_eq!(filtered_ids.value(2), 5);
        assert_eq!(filtered_names.value(0), "Alice");
        assert_eq!(filtered_names.value(1), "Charlie");
        assert_eq!(filtered_names.value(2), "Eve");
    }

    #[test]
    fn test_filter_batch_by_indices_with_nested_struct() {
        // Test filtering with nested struct types to ensure safe_take handles them correctly
        use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
        use datafusion::arrow::datatypes::Fields;

        // Create a schema with a nested struct column
        let inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Struct(inner_fields.clone()), false),
        ]));

        // Create data
        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));

        let x_array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let y_array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
        let struct_array: ArrayRef = Arc::new(
            StructArray::try_new(inner_fields.clone(), vec![x_array, y_array], None).unwrap(),
        );

        let batch = RecordBatch::try_new(schema, vec![id_array, struct_array]).unwrap();

        // Filter to keep rows 1, 3
        let indices = vec![1, 3];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        assert_eq!(filtered.num_rows(), 2);

        let filtered_ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(filtered_ids.value(0), 2);
        assert_eq!(filtered_ids.value(1), 4);

        let filtered_struct = filtered
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let filtered_x = filtered_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let filtered_y = filtered_struct
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(filtered_x.value(0), 20);
        assert_eq!(filtered_x.value(1), 40);
        assert_eq!(filtered_y.value(0), "b");
        assert_eq!(filtered_y.value(1), "d");
    }

    #[test]
    fn test_filter_batch_by_indices_with_list() {
        // Test filtering with list types to ensure safe_take handles them correctly
        use datafusion::arrow::array::{Int32Array, Int64Array, ListArray};
        use datafusion::arrow::buffer::OffsetBuffer;

        // Create a schema with a list column
        let list_field = Field::new("item", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "values",
                DataType::List(Arc::new(list_field.clone())),
                false,
            ),
        ]));

        // Create list data: [[1,2], [3,4,5], [6], [7,8,9,10]]
        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let values_data = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 5, 6, 10].into());
        let list_array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(list_field),
            offsets,
            Arc::new(values_data),
            None,
        ));

        let batch = RecordBatch::try_new(schema, vec![id_array, list_array]).unwrap();

        // Filter to keep rows 0, 2 ([1,2] and [6])
        let indices = vec![0, 2];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        assert_eq!(filtered.num_rows(), 2);

        let filtered_ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(filtered_ids.value(0), 1);
        assert_eq!(filtered_ids.value(1), 3);

        let filtered_list = filtered
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(filtered_list.len(), 2);

        // First list should have 2 elements [1, 2]
        let first_list = filtered_list.value(0);
        assert_eq!(first_list.len(), 2);

        // Second list should have 1 element [6]
        let second_list = filtered_list.value(1);
        assert_eq!(second_list.len(), 1);
    }

    #[test]
    fn test_filter_batch_by_indices_empty_indices() {
        // Test filtering with empty indices
        use datafusion::arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

        let batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();

        // Empty indices should return empty batch
        let indices: Vec<usize> = vec![];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn test_filter_batch_by_indices_repeated_indices() {
        // Test that repeated indices work (important for safe_take overflow handling)
        use datafusion::arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

        let batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();

        // Repeat indices to test safe_take's handling of repeated values
        let indices = vec![0, 0, 1, 1, 1, 2];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        assert_eq!(filtered.num_rows(), 6);

        let filtered_names = filtered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(filtered_names.value(0), "Alice");
        assert_eq!(filtered_names.value(1), "Alice");
        assert_eq!(filtered_names.value(2), "Bob");
        assert_eq!(filtered_names.value(3), "Bob");
        assert_eq!(filtered_names.value(4), "Bob");
        assert_eq!(filtered_names.value(5), "Charlie");
    }

    #[test]
    fn test_filter_batch_by_indices_preserves_metadata() {
        // Test that batch metadata is preserved through filtering
        use datafusion::arrow::array::{Int64Array, StringArray};
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        metadata.insert("test_key".to_string(), "test_value".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ],
            metadata.clone(),
        ));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

        let batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();

        let indices = vec![0, 2];
        let filtered = filter_batch_by_indices(&batch, &indices).unwrap();

        // Verify metadata is preserved
        let filtered_schema = filtered.schema();
        let filtered_metadata = filtered_schema.metadata();
        assert_eq!(
            filtered_metadata.get("test_key"),
            Some(&"test_value".to_string())
        );
    }
}

// Note: Comprehensive testing of batch processing is done via integration tests
// in `crates/streamling/tests/pipeline_postgres_sink.rs` which verify that batches
// are correctly processed, metrics are recorded, and checkpoint logic works.
// Full unit testing would require mocking sqlx::PgPool, CheckpointHandler, and MetricsRecorder.
