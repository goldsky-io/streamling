/// Simple State Backend backed by in-memory HashMap. Suite for testing and local development.
/// Note: configured namespace is not used in this implementation.
use crate::{StateBackendError, StateKey, StateOperatorBackend, StateOperatorBackendFactory};
use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::info;

pub struct InMemoryStateOperatorBackendFactory {}

impl InMemoryStateOperatorBackendFactory {
    pub fn new() -> Result<Self, StateBackendError> {
        Ok(InMemoryStateOperatorBackendFactory {})
    }
}

impl Default for InMemoryStateOperatorBackendFactory {
    fn default() -> Self {
        Self::new().expect("Failed to create InMemoryStateOperatorBackendFactory")
    }
}

impl StateOperatorBackendFactory for InMemoryStateOperatorBackendFactory {
    fn create<V>(&self, namespace: &str) -> Arc<dyn StateOperatorBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
    {
        Arc::new(InMemoryStateOperatorBackend::new(namespace))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct InMemoryStateOperatorBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    namespace: String,
    data: RwLock<HashMap<String, V>>,
}

impl<V> InMemoryStateOperatorBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    fn new(namespace: &str) -> Self {
        info!(
            "Creating a new in-memory state backend for namespace: {}",
            namespace
        );

        Self {
            namespace: namespace.to_string(),
            data: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<V> StateOperatorBackend<V> for InMemoryStateOperatorBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError> {
        Ok(self.data.read().get(&key.0).cloned())
    }

    async fn put(&self, key: StateKey, value: V) -> Result<(), StateBackendError> {
        self.data.write().insert(key.0, value);
        Ok(())
    }

    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError> {
        self.data.write().remove(&key.0);
        Ok(())
    }

    async fn clear(&self) -> Result<(), StateBackendError> {
        self.data.write().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_derive::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    struct TestStateString(String);

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_memory_state_operator_backend_with_strings() -> Result<(), StateBackendError> {
        let factory = InMemoryStateOperatorBackendFactory::new()?;

        let backend: Arc<dyn StateOperatorBackend<TestStateString>> =
            factory.create("test_namespace");

        backend
            .put(
                StateKey::from("key1"),
                TestStateString("value1".to_string()),
            )
            .await?;
        assert_eq!(
            backend.get(StateKey::from("key1")).await?,
            Some(TestStateString("value1".to_string()))
        );

        backend.remove(StateKey::from("key1")).await?;
        assert_eq!(backend.get(StateKey::from("key1")).await?, None);

        backend.clear().await?;

        Ok(())
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
    struct TestStateStruct {
        field1: String,
        field2: i32,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_memory_state_operator_backend_with_structs() -> Result<(), StateBackendError> {
        let factory = InMemoryStateOperatorBackendFactory::new()?;

        let backend: Arc<dyn StateOperatorBackend<TestStateStruct>> =
            factory.create("test_namespace");

        let state_struct = TestStateStruct {
            field1: "value1".to_string(),
            field2: 42,
        };

        backend
            .put(StateKey::from("key1"), state_struct.clone())
            .await?;
        assert_eq!(
            backend.get(StateKey::from("key1")).await?,
            Some(state_struct.clone())
        );

        backend.remove(StateKey::from("key1")).await?;
        assert_eq!(backend.get(StateKey::from("key1")).await?, None);

        backend.clear().await?;

        Ok(())
    }
}
