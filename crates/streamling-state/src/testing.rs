use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{StateBackendError, StateBackendErrorKind, StateKey, StateOperatorBackend};

pub enum FailCondition {
    Never,
    Always,
    OnKeyPrefix(String),
    FailOnNthCall(AtomicUsize, usize),
}

impl FailCondition {
    pub fn should_fail(&self, key: &str) -> bool {
        match self {
            FailCondition::Never => false,
            FailCondition::Always => true,
            FailCondition::OnKeyPrefix(prefix) => key.starts_with(prefix),
            FailCondition::FailOnNthCall(counter, n) => {
                let call = counter.fetch_add(1, Ordering::SeqCst) + 1;
                call == *n
            }
        }
    }
}

pub struct FailableStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de>,
{
    inner: Arc<dyn StateOperatorBackend<V>>,
    put_condition: Arc<RwLock<FailCondition>>,
}

impl<V> Debug for FailableStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailableStateBackend").finish()
    }
}

impl<V> FailableStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static,
{
    pub fn new(inner: Arc<dyn StateOperatorBackend<V>>) -> Self {
        Self {
            inner,
            put_condition: Arc::new(RwLock::new(FailCondition::Never)),
        }
    }

    pub fn with_put_condition(mut self, condition: FailCondition) -> Self {
        self.put_condition = Arc::new(RwLock::new(condition));
        self
    }
}

#[async_trait]
impl<V> StateOperatorBackend<V> for FailableStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static,
{
    async fn get(&self, key: StateKey) -> Result<Option<V>, StateBackendError> {
        self.inner.get(key).await
    }

    async fn put(&self, key: StateKey, value: V) -> Result<(), StateBackendError> {
        {
            let condition = self.put_condition.read().unwrap();
            if condition.should_fail(&key.0) {
                return Err(StateBackendError::new(
                    StateBackendErrorKind::Query,
                    "injected failure",
                ));
            }
        }
        self.inner.put(key, value).await
    }

    async fn remove(&self, key: StateKey) -> Result<(), StateBackendError> {
        self.inner.remove(key).await
    }

    async fn clear(&self) -> Result<(), StateBackendError> {
        self.inner.clear().await
    }
}
