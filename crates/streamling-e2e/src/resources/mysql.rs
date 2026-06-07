//! MySQL resource manager for creating isolated databases per test.

use crate::{E2eError, Result};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};
use std::time::Duration;
use tracing::info;

/// MySQL resource manager
pub struct MySqlResource {
    pool: MySqlPool,
    admin_pool: MySqlPool,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    base_url: String,
}

impl MySqlResource {
    /// Create a new MySQL resource with an isolated database
    pub async fn new(admin_url: &str, database: &str) -> Result<Self> {
        let parsed = url::Url::parse(admin_url)
            .map_err(|e| E2eError::Mysql(format!("invalid MySQL URL: {}", e)))?;

        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(3306);
        let user = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();

        let base_url = format!("mysql://{}:{}@{}:{}", user, password, host, port);

        let admin_pool = MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(30))
            .connect(admin_url)
            .await
            .map_err(|e| E2eError::Mysql(format!("failed to connect to MySQL: {}", e)))?;

        let create_db = format!("CREATE DATABASE IF NOT EXISTS `{}`", database);
        sqlx::query(&create_db)
            .execute(&admin_pool)
            .await
            .map_err(|e| E2eError::Mysql(format!("failed to create database: {}", e)))?;

        let db_url = format!("{}/{}", base_url, database);
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&db_url)
            .await
            .map_err(|e| E2eError::Mysql(format!("failed to connect to database: {}", e)))?;

        info!("Created MySQL database: {}", database);

        Ok(Self {
            pool,
            admin_pool,
            database: database.to_string(),
            host,
            port,
            user,
            password,
            base_url,
        })
    }

    pub fn connection_string(&self) -> String {
        format!("{}/{}", self.base_url, self.database)
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| E2eError::Mysql(format!("query failed: {}", e)))?;
        Ok(())
    }

    pub async fn count(&self, query: &str) -> Result<i64> {
        let row = sqlx::query(query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| E2eError::Mysql(format!("count query failed: {}", e)))?;
        let count: i64 = row.try_get(0).map_err(|e| E2eError::Mysql(e.to_string()))?;
        Ok(count)
    }

    pub async fn query<T>(&self, query: &str) -> Result<Vec<T>>
    where
        T: for<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> + Send + Unpin,
    {
        let rows = sqlx::query_as::<_, T>(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| E2eError::Mysql(format!("query failed: {}", e)))?;
        Ok(rows)
    }
}

impl Drop for MySqlResource {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let database = self.database.clone();
            let admin_pool = self.admin_pool.clone();
            let pool = self.pool.clone();

            handle.spawn(async move {
                pool.close().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", database);
                if let Err(e) = sqlx::query(&drop_sql).execute(&admin_pool).await {
                    tracing::warn!("Failed to drop MySQL database {}: {}", database, e);
                } else {
                    info!("Dropped MySQL database: {}", database);
                }
                admin_pool.close().await;
            });
        }
    }
}
