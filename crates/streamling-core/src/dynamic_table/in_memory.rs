use crate::dynamic_table::{DynamicTableBackend, DynamicTableBackendError, extract_string_values};
use async_trait::async_trait;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::trace;

/// An in-memory implementation of the DynamicTableBackend trait.
/// The simplest backend that can be used for testing.
#[derive(Debug)]
pub struct InMemoryDynamicTableBackend {
    backend_entity_name: String,
    storage: RwLock<HashSet<String>>,
    _max_batch_size: usize, // Accepted but not used for in-memory (no I/O benefit)
}

impl InMemoryDynamicTableBackend {
    pub fn new(
        backend_entity_name: String,
        _schema: Option<String>,
        _column: Option<String>,
        _time_column: Option<String>,
        max_batch_size: usize,
    ) -> Self {
        // Schema, column, and time_column are accepted but ignored for in-memory backend
        // as it doesn't use schema/column concepts
        // max_batch_size is also accepted but not used since in-memory operations are fast
        Self {
            backend_entity_name,
            storage: RwLock::new(HashSet::new()),
            _max_batch_size: max_batch_size,
        }
    }
}

#[async_trait]
impl DynamicTableBackend for InMemoryDynamicTableBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append(&self, values: ArrayRef) -> Result<(), DynamicTableBackendError> {
        let values = extract_string_values(values)?;

        let mut table = self.storage.write();
        for value in values {
            table.insert(value.clone());
            trace!(
                "[append] for table name '{}' with value '{}'",
                self.backend_entity_name, value
            );
        }
        Ok(())
    }

    async fn contains(&self, values: ArrayRef) -> Result<ArrayRef, DynamicTableBackendError> {
        // Cast ArrayRef to StringArray
        let string_array = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DynamicTableBackendError::StringArrayExpected)?;

        let table = self.storage.read();
        let mut builder = BooleanBuilder::with_capacity(string_array.len());

        for i in 0..string_array.len() {
            if !string_array.is_null(i) {
                let value = string_array.value(i);
                let result = table.contains(value);
                builder.append_value(result);
                trace!(
                    "[contains] for table name '{}' with value '{}' result '{:?}'",
                    self.backend_entity_name, value, result
                );
            } else {
                builder.append_null();
            }
        }

        let boolean_array = builder.finish();
        Ok(Arc::new(boolean_array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::array::builder::StringBuilder;

    // Helper methods to reduce test duplication
    fn create_string_array(values: &[&str]) -> ArrayRef {
        let mut builder = StringBuilder::new();
        for value in values {
            builder.append_value(*value);
        }
        Arc::new(builder.finish())
    }

    async fn assert_contains(
        backend: &InMemoryDynamicTableBackend,
        values: &[&str],
        expected_results: &[bool],
    ) -> Result<(), DynamicTableBackendError> {
        let query_array = create_string_array(values);
        let result = backend.contains(query_array).await?;
        let boolean_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(boolean_array.len(), expected_results.len());
        for (i, &expected) in expected_results.iter().enumerate() {
            assert_eq!(
                boolean_array.value(i),
                expected,
                "Mismatch for value '{}' at index {}: expected {}, got {}",
                values[i],
                i,
                expected,
                boolean_array.value(i)
            );
        }
        Ok(())
    }

    async fn append_values(
        backend: &InMemoryDynamicTableBackend,
        values: &[&str],
    ) -> Result<(), DynamicTableBackendError> {
        let array = create_string_array(values);
        backend.append(array).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_memory_dynamic_table_backend_basic_operations()
    -> Result<(), DynamicTableBackendError> {
        let _ = tracing_subscriber::fmt::try_init();

        let table_one_backend =
            InMemoryDynamicTableBackend::new(String::from("table1"), None, None, None, 10000);
        let table_two_backend =
            InMemoryDynamicTableBackend::new(String::from("table2"), None, None, None, 10000);

        // Test append and contains
        append_values(&table_one_backend, &["value1"]).await?;
        assert_contains(&table_one_backend, &["value1"], &[true]).await?;
        assert_contains(&table_one_backend, &["value2"], &[false]).await?;

        // Test different tables
        append_values(&table_two_backend, &["value1"]).await?;
        assert_contains(&table_two_backend, &["value1"], &[true]).await?;
        assert_contains(&table_one_backend, &["value1"], &[true]).await?;
        assert_contains(&table_two_backend, &["value2"], &[false]).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_memory_dynamic_table_backend_duplicate_values()
    -> Result<(), DynamicTableBackendError> {
        let table_one_backend =
            InMemoryDynamicTableBackend::new(String::from("table1"), None, None, None, 10000);

        // Test appending the same value multiple times (should be deduplicated by HashSet)
        append_values(&table_one_backend, &["value1"]).await?;
        append_values(&table_one_backend, &["value1"]).await?;
        append_values(&table_one_backend, &["value1"]).await?;

        assert_contains(&table_one_backend, &["value1"], &[true]).await?;

        // Add another value to verify the set still works correctly
        append_values(&table_one_backend, &["value2"]).await?;
        assert_contains(&table_one_backend, &["value1", "value2"], &[true, true]).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_memory_dynamic_table_backend_multiple_values()
    -> Result<(), DynamicTableBackendError> {
        let table_backend =
            InMemoryDynamicTableBackend::new(String::from("table1"), None, None, None, 10000);

        // Append multiple values at once
        append_values(&table_backend, &["value1", "value2", "value3"]).await?;

        // Test contains with multiple values - some exist, some don't
        assert_contains(
            &table_backend,
            &["value1", "value4", "value2"],
            &[true, false, true],
        )
        .await?;

        Ok(())
    }
}
