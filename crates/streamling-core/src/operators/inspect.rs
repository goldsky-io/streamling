use crate::formats::FromArrowConverter;
use crate::formats::json::FromArrowToJsonConverter;
use arrow::array::RecordBatch;
use dashmap::DashMap;
use fixed_deque::Deque;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use streamling_config::LiveDataInspectConfig;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// A single live data event containing the topology node key and the JSON data
#[derive(Debug, Clone)]
pub struct LiveDataEvent {
    pub topology_node_key: String,
    pub data: String,
}

pub struct LiveDataInspect {
    capacity: Arc<DashMap<String, AtomicU32>>,
    storage: DashMap<String, Deque<String>>,
    insert_timings: Arc<DashMap<String, Vec<Instant>>>,
    record_batch_to_string_converter: FromArrowToJsonConverter,
    _background_task: JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Broadcast channel for streaming new events to SSE clients
    /// Using a large buffer (1000) to handle bursts of data
    broadcast_tx: broadcast::Sender<LiveDataEvent>,
}

static INSTANCE: OnceLock<LiveDataInspect> = OnceLock::new();

impl LiveDataInspect {
    /// Create a new LiveDataInspector with default settings
    pub fn new(config: LiveDataInspectConfig, topology_node_keys: Vec<String>) -> Self {
        let refresh_in_secs = config.refresh_in_secs;
        let records_per_topology_node = config.records_per_topology_node;
        let storage = DashMap::with_capacity(topology_node_keys.len());
        let capacity = Arc::new(DashMap::<String, AtomicU32>::with_capacity(
            topology_node_keys.len(),
        ));
        let insert_timings = Arc::new(DashMap::<String, Vec<Instant>>::with_capacity(
            topology_node_keys.len(),
        ));
        topology_node_keys.iter().for_each(|key| {
            capacity.insert(
                String::from(key),
                AtomicU32::from(records_per_topology_node),
            );
            insert_timings.insert(String::from(key), vec![]);
            storage.insert(
                String::from(key),
                Deque::new(records_per_topology_node as usize),
            );
        });
        // Clone references for the background task before moving
        let capacity_clone = capacity.clone();
        let insert_timings_clone = insert_timings.clone();

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Create broadcast channel for SSE events
        // Ensure minimum capacity of 1 to avoid panic when no topology nodes are configured
        let channel_capacity = std::cmp::max(
            1,
            topology_node_keys.len() * records_per_topology_node as usize,
        );
        let (broadcast_tx, _) = broadcast::channel(channel_capacity);

        // Start a background task to maintain capacity only if there are topology nodes
        let background_task = if topology_node_keys.is_empty() {
            // No work to do, spawn a task that immediately completes
            tokio::spawn(async {})
        } else {
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(refresh_in_secs as u64));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            for mut entry in insert_timings_clone.iter_mut() {
                                let (topology_node_key, timings) = entry.pair_mut();
                                let size_before_prune = timings.len();
                                timings.retain(|insert_time| {
                                    Instant::now().duration_since(*insert_time)
                                        < Duration::from_secs(refresh_in_secs as u64)
                                });
                                let pruned_count = size_before_prune - timings.len();
                                debug!(
                                    "Pruned {} entries for topology_node_key: {}",
                                    pruned_count, topology_node_key
                                );
                                let capacity = capacity_clone.get(topology_node_key).unwrap();
                                capacity.fetch_add(pruned_count as u32, Ordering::SeqCst);
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("LiveDataInspect background task received shutdown signal");
                                break;
                            }
                        }
                    }
                }
                info!("LiveDataInspect background task terminated");
            })
        };
        info!(
            "Initialized LiveDataInspect for topology_node_keys:{}",
            topology_node_keys.join(", ")
        );
        Self {
            storage,
            capacity,
            insert_timings,
            record_batch_to_string_converter: FromArrowToJsonConverter::new(),
            _background_task: background_task,
            shutdown_tx,
            broadcast_tx,
        }
    }

    /// Get the singleton instance of LiveDataInspector
    pub fn create_instance(
        config: LiveDataInspectConfig,
        topology_node_keys: Vec<String>,
    ) -> &'static LiveDataInspect {
        INSTANCE.get_or_init(|| LiveDataInspect::new(config, topology_node_keys))
    }

    pub fn get_instance() -> &'static LiveDataInspect {
        INSTANCE.get_or_init(|| {
            LiveDataInspect::new(
                LiveDataInspectConfig {
                    refresh_in_secs: 30,
                    records_per_topology_node: 15,
                },
                vec![],
            )
        })
    }

    /// Gracefully shutdown the background task
    pub async fn shutdown(&self) {
        debug!("Shutting down LiveDataInspect background task");
        let _ = self.shutdown_tx.send(true);
        // Note: We don't await the background_task here because it's stored as a field,
        // and we can't move it out. The task will terminate when it receives the signal.
    }

    /// Subscribe to live data events for SSE streaming
    /// Returns a receiver that will get new events as they arrive
    pub fn subscribe(&self) -> broadcast::Receiver<LiveDataEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Check if there are any active subscribers
    fn has_subscribers(&self) -> bool {
        self.broadcast_tx.receiver_count() > 0
    }

    pub async fn process(&self, topology_node_key: &str, batch: &RecordBatch) {
        if let Some(capacity) = self.capacity.get(topology_node_key) {
            let capacity = capacity.value().load(Ordering::SeqCst);
            let to_store = capacity.min(batch.num_rows() as u32);
            if to_store > 0 {
                debug!(
                    "Attempt to store {} records in storage for topology_node_key: {}",
                    to_store, topology_node_key
                );
                let batch = batch.slice(0, to_store as usize);
                let rows = self
                    .record_batch_to_string_converter
                    .convert_from_batch(&batch)
                    .unwrap_or_else(|err| {
                        warn!("Failed to convert record batch to string. Error: {}", err);
                        vec![]
                    });

                if let Some(mut entry) = self.storage.get_mut(topology_node_key) {
                    let queue = entry.value_mut();
                    let should_broadcast = self.has_subscribers();

                    // Fixed deque automatically handles eviction when at capacity
                    for row in rows.into_iter() {
                        let json_row = String::from_utf8(row).unwrap();
                        queue.push_front(json_row.clone());
                        self.capacity
                            .get(topology_node_key)
                            .unwrap()
                            .fetch_sub(1, Ordering::SeqCst);
                        // Track insert timing for background task eviction
                        if let Some(mut timings_entry) =
                            self.insert_timings.get_mut(topology_node_key)
                        {
                            timings_entry.push(Instant::now());
                        }

                        // Broadcast new event to SSE clients if there are any active subscribers
                        if should_broadcast {
                            let event = LiveDataEvent {
                                topology_node_key: topology_node_key.to_string(),
                                data: json_row,
                            };
                            // We ignore send errors - if no one is listening, that's fine
                            let _ = self.broadcast_tx.send(event);
                        }
                    }
                    debug!(
                        "Successfully stored {} row in storage for topology_node_key: {}",
                        to_store, topology_node_key
                    );
                }
            }
        }
    }

    /// Get stored data for a topology_node_key
    pub fn get_stored_data(&self, topology_node_key: &str) -> Option<Vec<String>> {
        self.storage
            .get(topology_node_key)
            .map(|entry| entry.value().iter().cloned().collect())
    }

    /// Get current capacity for a topology_node_key (for testing/debugging)
    #[cfg(test)]
    pub fn get_capacity(&self, topology_node_key: &str) -> Option<u32> {
        self.capacity
            .get(topology_node_key)
            .map(|atomic| atomic.load(Ordering::SeqCst))
    }

    /// Get the number of stored records for topology_node_key
    pub fn get_stored_count(&self, topology_node_key: &str) -> usize {
        self.storage
            .get(topology_node_key)
            .map(|entry| entry.value().len())
            .unwrap_or(0)
    }

    /// Get all topology node keys that are being tracked
    pub fn get_all_topology_node_keys(&self) -> Vec<String> {
        self.storage
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]))
    }

    fn create_test_batch(start_id: i32, count: usize) -> RecordBatch {
        let schema = create_test_schema();

        let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
        let names: Vec<Option<String>> = (0..count)
            .map(|i| Some(format!("name_{}", start_id + i as i32)))
            .collect();
        let values: Vec<Option<i32>> = (0..count)
            .map(|i| Some((start_id + i as i32) * 10))
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_data_insertion() {
        let refresh_in_secs = 5;
        let records_per_topology_node = 5;
        let topology_node_key = "test_operator";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(topology_node_key)],
        );
        let batch = create_test_batch(1, 3);

        // Process the batch
        inspector.process(topology_node_key, &batch).await;

        let stored_data = inspector.get_stored_data(topology_node_key);
        assert!(stored_data.is_some(), "Data should be stored");

        let data = stored_data.unwrap();
        assert_eq!(data.len(), 3, "Should have 3 records stored");

        assert!(data[0].contains("\"id\":3"), "First record should be id 3");
        assert!(data[1].contains("\"id\":2"), "Second record should be id 2");
        assert!(data[2].contains("\"id\":1"), "Third record should be id 1");

        let capacity = inspector.get_capacity(topology_node_key);
        assert_eq!(capacity, Some(2), "Capacity should be reduced by 3");
    }

    #[tokio::test]
    async fn test_capacity_limits() {
        let refresh_in_secs = 10;
        let records_per_topology_node = 5;
        let topology_node_key = "test_capacity";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(topology_node_key)],
        );

        // Fill up to capacity (100 records)
        let batch = create_test_batch(1, 10);
        inspector.process(topology_node_key, &batch).await;

        // Verify all records stored
        assert_eq!(inspector.get_stored_count(topology_node_key), 5);
        assert_eq!(inspector.get_capacity(topology_node_key), Some(0));

        // Try to add more records - should not store any
        let batch2 = create_test_batch(11, 10);
        inspector.process(topology_node_key, &batch2).await;
        //should still have 5 records
        assert_eq!(inspector.get_stored_count(topology_node_key), 5);
        assert_eq!(inspector.get_capacity(topology_node_key), Some(0));
        let stored_data = inspector.get_stored_data(topology_node_key);
        let data = stored_data.unwrap();
        assert!(data[4].contains("\"id\":1"), "Last record should be id 1");
    }

    #[tokio::test]
    async fn test_eviction_age_expire() {
        let refresh_in_secs = 2;
        let records_per_topology_node = 5;
        let topology_node_key = "test_capacity";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(topology_node_key)],
        );
        let batch = create_test_batch(1, 10);
        inspector.process(topology_node_key, &batch).await;

        // Initially, capacity should be 0 (all used up)
        assert_eq!(inspector.get_capacity(topology_node_key), Some(0));

        // Wait for a background task to run and restore capacity
        sleep(Duration::from_secs(refresh_in_secs as u64 + 1)).await;

        // Records should still exist as expiration only happens after new batch arrives
        // but capacity should increase as the eviction age has expired and background task ran
        assert_eq!(inspector.get_stored_count(topology_node_key), 5);
        assert_eq!(
            inspector.get_capacity(topology_node_key),
            Some(records_per_topology_node)
        );
    }

    #[tokio::test]
    async fn test_new_batch_after_eviction_age_expire() {
        let refresh_in_secs = 2;
        let records_per_topology_node = 5;
        let topology_node_key = "test_capacity";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(topology_node_key)],
        );
        let batch = create_test_batch(1, 10);
        inspector.process(topology_node_key, &batch).await;

        assert_eq!(inspector.get_capacity(topology_node_key), Some(0));

        // Wait for a background task to run and restore capacity
        sleep(Duration::from_secs(refresh_in_secs as u64 + 1)).await;
        let batch = create_test_batch(11, 3);
        inspector.process(topology_node_key, &batch).await;

        assert_eq!(inspector.get_stored_count(topology_node_key), 5);
        //the 2 records from the first batch are expired but not evicted
        assert_eq!(inspector.get_capacity(topology_node_key), Some(2));
        let stored_data = inspector.get_stored_data(topology_node_key);
        assert!(stored_data.is_some(), "Data should be stored");

        let data = stored_data.unwrap();
        assert_eq!(data.len(), 5, "Should have 5 records stored");

        assert!(data[0].contains("\"id\":13"));
        assert!(data[1].contains("\"id\":12"));
        assert!(data[2].contains("\"id\":11"));
        assert!(data[3].contains("\"id\":5"));
        assert!(data[4].contains("\"id\":4"));
        let batch = create_test_batch(14, 10);
        inspector.process(topology_node_key, &batch).await;

        assert_eq!(inspector.get_stored_count(topology_node_key), 5);
        //the 2 records from the first batch are expired but not evicted
        assert_eq!(inspector.get_capacity(topology_node_key), Some(0));
        let data = inspector.get_stored_data(topology_node_key).unwrap();
        assert_eq!(data.len(), 5, "Should have 5 records stored");
        assert!(data[0].contains("\"id\":15"));
        assert!(data[1].contains("\"id\":14"));
        assert!(data[2].contains("\"id\":13"));
        assert!(data[3].contains("\"id\":12"));
        assert!(data[4].contains("\"id\":11"));
    }

    #[tokio::test]
    async fn test_multiple_operators() {
        let refresh_in_secs = 2;
        let records_per_topology_node = 5;
        // Test with multiple reference names
        let ref1 = "operator_1";
        let ref2 = "operator_2";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(ref1), String::from(ref2)],
        );

        let batch1 = create_test_batch(1, 3);
        let batch2 = create_test_batch(100, 2);

        inspector.process(ref1, &batch1).await;
        inspector.process(ref2, &batch2).await;

        assert_eq!(inspector.get_stored_count(ref1), 3);
        assert_eq!(inspector.get_stored_count(ref2), 2);

        assert_eq!(inspector.get_capacity(ref1), Some(2));
        assert_eq!(inspector.get_capacity(ref2), Some(3));

        // Verify data isolation
        let data1 = inspector.get_stored_data(ref1).unwrap();
        let data2 = inspector.get_stored_data(ref2).unwrap();

        assert!(data1[0].contains("\"id\":3"));
        assert!(data2[0].contains("\"id\":101"));
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let refresh_in_secs = 2;
        let records_per_topology_node = 5;
        let topology_node_key = "test_empty";
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            vec![String::from(topology_node_key)],
        );
        let schema = create_test_schema();
        let empty_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            ],
        )
        .unwrap();

        inspector.process(topology_node_key, &empty_batch).await;
        assert_eq!(inspector.get_stored_count(topology_node_key), 0);
        assert_eq!(
            inspector.get_capacity(topology_node_key),
            Some(records_per_topology_node)
        );
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let refresh_in_secs = 2;
        let records_per_topology_node = 30;
        let mut topology_node_keys = vec![];
        for i in 1..6 {
            topology_node_keys.push(format!("operator_{}", i));
        }
        let inspector = Arc::new(LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            topology_node_keys,
        ));
        let mut handles = vec![];

        // Spawn multiple tasks processing different operators
        for i in 1..6 {
            let inspector_clone = inspector.clone();
            let handle = tokio::spawn(async move {
                let topology_node_key = format!("operator_{}", i);
                let batch = create_test_batch((i * records_per_topology_node) as i32, 20);
                inspector_clone.process(&topology_node_key, &batch).await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify each operator has its data
        for i in 1..6 {
            let topology_node_key = format!("operator_{}", i);
            assert_eq!(inspector.get_stored_count(&topology_node_key), 20);
            assert_eq!(inspector.get_capacity(&topology_node_key), Some(10));
        }
    }

    #[tokio::test]
    async fn test_singleton_behavior() {
        let instance1 = LiveDataInspect::create_instance(
            LiveDataInspectConfig {
                records_per_topology_node: 5,
                refresh_in_secs: 10,
            },
            vec![String::from("singleton_test")],
        );
        let instance2 = LiveDataInspect::get_instance();

        assert!(
            std::ptr::eq(instance1, instance2),
            "Should return the same singleton instance"
        );

        // Test that data persists across instance calls
        let batch = create_test_batch(1, 10);
        instance1.process("singleton_test", &batch).await;

        // Should be able to access the same data from instance2
        assert_eq!(instance2.get_stored_count("singleton_test"), 5);
    }

    #[tokio::test]
    async fn test_empty_topology_nodes_no_panic() {
        // Test that creating LiveDataInspect with empty topology_node_keys doesn't panic
        // This reproduces the scenario when live data inspect is disabled
        let inspector = LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs: 30,
                records_per_topology_node: 15,
            },
            vec![], // Empty topology nodes - this used to cause panic
        );

        // Should be able to check subscribers without panic (initially no subscribers)
        assert!(!inspector.has_subscribers());

        // Should be able to subscribe without panic
        let _receiver = inspector.subscribe();

        // Now should have subscribers
        assert!(inspector.has_subscribers());

        // Should be able to get all topology node keys (empty)
        assert_eq!(inspector.get_all_topology_node_keys(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_concurrent_processing_with_capacity_limits() {
        use rand::Rng;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::time::{Duration, Instant, sleep};

        let refresh_in_secs = 5; // Long eviction time to focus on capacity limits
        let records_per_topology_node = 50;
        let topology_node_keys = vec![
            "operator_alpha".to_string(),
            "operator_beta".to_string(),
            "operator_gamma".to_string(),
            "operator_delta".to_string(),
        ];

        let inspector = Arc::new(LiveDataInspect::new(
            LiveDataInspectConfig {
                refresh_in_secs,
                records_per_topology_node,
            },
            topology_node_keys.clone(),
        ));

        // 4 unique reference names
        let topology_node_keys = vec![
            "operator_alpha".to_string(),
            "operator_beta".to_string(),
            "operator_gamma".to_string(),
            "operator_delta".to_string(),
        ];

        // Mapping threads to reference names (some threads share reference names)
        let thread_reference_mapping = [
            topology_node_keys[0].clone(), // Thread 0 -> operator_alpha
            topology_node_keys[0].clone(), // Thread 1 -> operator_alpha (shared)
            topology_node_keys[1].clone(), // Thread 2 -> operator_beta
            topology_node_keys[1].clone(), // Thread 3 -> operator_beta (shared)
            topology_node_keys[2].clone(), // Thread 4 -> operator_gamma
            topology_node_keys[2].clone(), // Thread 5 -> operator_gamma (shared)
            topology_node_keys[3].clone(), // Thread 6 -> operator_delta
        ];

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];

        // Spawn 7 processing threads
        for (thread_id, _) in thread_reference_mapping.iter().enumerate() {
            let inspector_clone = inspector.clone();
            let topology_node_key = thread_reference_mapping[thread_id].clone();
            let shutdown_flag_clone = shutdown_flag.clone();

            let handle = tokio::spawn(async move {
                use rand::SeedableRng;
                let mut rng = rand::rngs::StdRng::from_entropy();
                let mut batch_counter = 0;

                while !shutdown_flag_clone.load(Ordering::Relaxed) {
                    // Create a batch of 300 rows
                    let batch = create_test_batch(batch_counter * 300, 300);
                    batch_counter += 1;

                    // Process the batch
                    inspector_clone.process(&topology_node_key, &batch).await;

                    // Random sleep between 300 ms to 3 seconds
                    let sleep_duration = rng.gen_range(300..=3000);
                    sleep(Duration::from_millis(sleep_duration)).await;
                }

                println!(
                    "Thread {} with reference '{}' shutting down after {} batches",
                    thread_id, topology_node_key, batch_counter
                );
            });

            handles.push(handle);
        }

        // Assertion loop - run for 30 seconds
        let assertion_start = Instant::now();
        let assertion_duration = Duration::from_secs(30);

        while assertion_start.elapsed() < assertion_duration {
            // Check that the stored count doesn't exceed the records_per_topology_node for each reference name
            for topology_node_key in &topology_node_keys {
                let stored_count = inspector.get_stored_count(topology_node_key);
                assert!(
                    stored_count <= records_per_topology_node as usize,
                    "Reference '{}' has {} stored records, which exceeds limit of {}",
                    topology_node_key,
                    stored_count,
                    records_per_topology_node
                );
            }
            // Check every 100 ms
            sleep(Duration::from_millis(100)).await;
        }

        println!("Assertion period completed. Shutting down processing threads...");

        // Signal threads to shut down
        shutdown_flag.store(true, Ordering::Relaxed);

        // Wait for all threads to complete
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(_) => println!("Thread {} completed successfully", i),
                Err(e) => eprintln!("Thread {} failed: {:?}", i, e),
            }
        }

        // Final verification
        println!("Final state:");
        for topology_node_key in &topology_node_keys {
            let stored_count = inspector.get_stored_count(topology_node_key);
            let capacity = inspector.get_capacity(topology_node_key);
            println!(
                "Reference '{}': {} stored records, capacity: {:?}",
                topology_node_key, stored_count, capacity
            );

            assert!(
                stored_count <= records_per_topology_node as usize,
                "Final check: Reference '{}' has {} stored records, exceeds limit of {}",
                topology_node_key,
                stored_count,
                records_per_topology_node
            );
        }

        println!("Test completed successfully!");
    }
}
