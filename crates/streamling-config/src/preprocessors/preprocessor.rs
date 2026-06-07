use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PreprocessorError {
    #[error("YAML parsing error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Plugin preprocessor error: {0}")]
    PluginError(String),
}

pub struct TopologyPreprocessor {
    preprocessors: Vec<Box<dyn Preprocessor>>,
}

impl TopologyPreprocessor {
    pub fn new(preprocessors: Vec<Box<dyn Preprocessor>>) -> Self {
        Self { preprocessors }
    }

    pub async fn preprocess_topology(&self, config: String) -> Result<String, PreprocessorError> {
        stream::iter(&self.preprocessors)
            .fold(Ok(config), |acc, preprocessor| async move {
                let cfg = acc?;
                preprocessor.preprocess_topology(cfg).await
            })
            .await
    }
}

#[async_trait]
pub trait Preprocessor: Send + Sync {
    async fn preprocess_topology(&self, config: String) -> Result<String, PreprocessorError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::future::Future;
    use std::pin::Pin;

    struct FnPreprocessor<F>
    where
        F: Send
            + Sync
            + 'static
            + Fn(String) -> Pin<Box<dyn Future<Output = Result<String, PreprocessorError>> + Send>>,
    {
        func: F,
    }

    #[async_trait]
    impl<F> Preprocessor for FnPreprocessor<F>
    where
        F: Send
            + Sync
            + 'static
            + Fn(String) -> Pin<Box<dyn Future<Output = Result<String, PreprocessorError>> + Send>>,
    {
        async fn preprocess_topology(&self, config: String) -> Result<String, PreprocessorError> {
            (self.func)(config).await
        }
    }

    #[tokio::test]
    async fn test_inline_topology_preprocessor() {
        let preprocessors: Vec<Box<dyn Preprocessor>> = vec![
            Box::new(FnPreprocessor {
                func: |conf: String| Box::pin(async move { Ok(conf.replace("foo", "bar")) }),
            }),
            Box::new(FnPreprocessor {
                func: |conf: String| Box::pin(async move { Ok(conf.replace("baz", "BAZ")) }),
            }),
        ];
        let topology_preprocessor = TopologyPreprocessor::new(preprocessors);
        let result = topology_preprocessor
            .preprocess_topology("foo baz".to_string())
            .await
            .unwrap();
        assert_eq!(result, "bar BAZ".to_string());
    }
}
