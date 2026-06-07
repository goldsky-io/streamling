//! ClickHouse resource manager for creating isolated databases per test.

use crate::{E2eError, Result};
use clickhouse::{Client, Row};
use serde::Deserialize;
use tracing::info;

/// ClickHouse resource manager
pub struct ClickHouseResource {
    /// ClickHouse client
    client: Client,
    /// Admin client (connected to default database)
    admin_client: Client,
    /// Name of the isolated database
    pub database: String,
    /// Base URL
    url: String,
    /// Whether to drop the database on cleanup
    should_drop: bool,
}

impl ClickHouseResource {
    /// Connect to an existing ClickHouse database (for inspection, does not drop on cleanup)
    pub async fn connect_existing(url: &str, database: &str) -> Result<Self> {
        // Create client connected to the existing database
        let client = Client::default().with_url(url).with_database(database);

        // Create admin client (won't be used for dropping)
        let admin_client = Client::default().with_url(url).with_database("default");

        info!("Connected to existing ClickHouse database: {}", database);

        Ok(Self {
            client,
            admin_client,
            database: database.to_string(),
            url: url.to_string(),
            should_drop: false, // Don't drop when connecting to existing database
        })
    }

    /// Create a new ClickHouse resource with an isolated database
    pub async fn new(url: &str, database: &str) -> Result<Self> {
        // Create admin client connected to default database
        let admin_client = Client::default().with_url(url).with_database("default");

        // Create the isolated database
        admin_client
            .query(&format!("CREATE DATABASE IF NOT EXISTS `{}`", database))
            .execute()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;

        info!("Created ClickHouse database: {}", database);

        // Create client connected to the isolated database
        let client = Client::default().with_url(url).with_database(database);

        Ok(Self {
            client,
            admin_client,
            database: database.to_string(),
            url: url.to_string(),
            should_drop: true, // Drop when created by test
        })
    }

    /// Get the ClickHouse client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Execute a SQL statement
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.client
            .query(sql)
            .execute()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;
        Ok(())
    }

    /// Execute a count query and return the result
    pub async fn count(&self, query: &str) -> Result<u64> {
        let count: u64 = self
            .client
            .query(query)
            .fetch_one()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;
        Ok(count)
    }

    /// Execute a query and fetch all rows
    pub async fn query<T>(&self, query: &str) -> Result<Vec<T>>
    where
        T: Row + for<'a> Deserialize<'a>,
    {
        let rows: Vec<T> = self
            .client
            .query(query)
            .fetch_all()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;
        Ok(rows)
    }

    /// Execute a query and fetch one row
    pub async fn query_one<T>(&self, query: &str) -> Result<T>
    where
        T: Row + for<'a> Deserialize<'a>,
    {
        let row: T = self
            .client
            .query(query)
            .fetch_one()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;
        Ok(row)
    }

    /// Get the column types for a table
    pub async fn get_column_types(&self, table: &str) -> Result<Vec<(String, String)>> {
        #[derive(Row, Deserialize)]
        struct ColumnInfo {
            name: String,
            #[serde(rename = "type")]
            col_type: String,
        }

        let query = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            self.database, table
        );
        let columns: Vec<ColumnInfo> = self
            .client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;

        Ok(columns.into_iter().map(|c| (c.name, c.col_type)).collect())
    }

    /// Get the connection URL for this database
    pub fn connection_url(&self) -> String {
        format!("{}?database={}", self.url, self.database)
    }

    /// List all tables in the database
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        #[derive(Row, Deserialize)]
        struct TableName {
            name: String,
        }

        let tables: Vec<TableName> = self
            .query(&format!(
                "SELECT name FROM system.tables WHERE database = '{}' ORDER BY name",
                self.database
            ))
            .await?;

        Ok(tables.into_iter().map(|t| t.name).collect())
    }

    /// Get sample data from a table using HTTP API (for better formatting)
    pub async fn get_sample_data_formatted(&self, table: &str, limit: usize) -> Result<String> {
        let sample_query = format!(
            "SELECT * FROM {}.{} LIMIT {} FORMAT PrettyCompact",
            self.database, table, limit
        );

        let response = reqwest::Client::new()
            .post(&self.url)
            .query(&[("database", self.database.as_str())])
            .body(sample_query)
            .send()
            .await
            .map_err(|e| E2eError::ClickHouse(e.to_string()))?;

        if response.status().is_success() {
            let text = response
                .text()
                .await
                .map_err(|e| E2eError::ClickHouse(e.to_string()))?;
            Ok(text)
        } else {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            Err(E2eError::ClickHouse(format!(
                "HTTP {}: {}",
                status, error_text
            )))
        }
    }
}

impl Drop for ClickHouseResource {
    fn drop(&mut self) {
        // Only drop if this resource was created (not connected to existing)
        if !self.should_drop {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let database = self.database.clone();
            let admin_client = self.admin_client.clone();

            handle.spawn(async move {
                let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", database);
                if let Err(e) = admin_client.query(&drop_sql).execute().await {
                    tracing::warn!("Failed to drop ClickHouse database {}: {}", database, e);
                } else {
                    info!("Dropped ClickHouse database: {}", database);
                }
            });
        }
    }
}
