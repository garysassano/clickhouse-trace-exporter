use crate::error::ClickhouseExporterError;
use std::time::Duration;
use url::Url;

// Sensible defaults for table names
const DEFAULT_SPANS_TABLE: &str = "otel_spans";
// const DEFAULT_ATTRIBUTES_TABLE: &str = "otel_span_attributes"; // Optional: If implementing flattened attributes
// const DEFAULT_ERRORS_TABLE: &str = "otel_span_errors"; // Removed unused errors table

/// Configuration for the ClickHouse Exporter.
#[derive(Debug, Clone)]
pub struct ClickhouseExporterConfig {
    pub(crate) dsn: Url,
    pub(crate) spans_table_name: String,
    pub(crate) cluster_name: Option<String>,
    pub(crate) table_engine: String,
    pub(crate) table_engine_params: Option<String>,
    pub(crate) ttl: Option<Duration>,
    // pub(crate) attributes_table_name: String, // Optional
    // pub(crate) errors_table_name: String, // Removed unused errors table
    pub(crate) create_schema: bool,
    // Add other options like:
    // - database_name (if not in DSN)
    // - insert_timeout
    // - connection_pool_options (if needed beyond basic DSN)
    // Go Exporter options not yet implemented:
    // - TTL
    // - Retry/Queue settings (handled by SDK batch processor to some extent)
    // - Async Insert flag (client.insert() might handle this implicitly or via DSN params)
}

impl ClickhouseExporterConfig {
    /// Creates a new configuration with default table names.
    /// Requires a valid ClickHouse DSN (e.g., "tcp://user:pass@host:port/db?compression=lz4").
    pub fn new(dsn: String) -> Result<Self, ClickhouseExporterError> {
        let parsed_dsn = Url::parse(&dsn)?;
        Ok(ClickhouseExporterConfig {
            dsn: parsed_dsn,
            spans_table_name: DEFAULT_SPANS_TABLE.to_string(),
            cluster_name: None,
            table_engine: "MergeTree".to_string(),
            table_engine_params: None,
            ttl: None,
            // attributes_table_name: DEFAULT_ATTRIBUTES_TABLE.to_string(), // Optional
            // errors_table_name: DEFAULT_ERRORS_TABLE.to_string(), // Removed unused errors table
            create_schema: false, // Default to not creating schema
        })
    }

    /// Sets the name for the main spans table.
    pub fn with_spans_table(mut self, name: impl Into<String>) -> Self {
        self.spans_table_name = name.into();
        self
    }

    /// Sets the cluster name for ON CLUSTER clause when creating tables.
    pub fn with_cluster(mut self, name: impl Into<String>) -> Self {
        self.cluster_name = Some(name.into());
        self
    }

    /// Sets the ClickHouse table engine and its parameters.
    pub fn with_table_engine(
        mut self,
        engine: impl Into<String>,
        params: impl Into<String>,
    ) -> Self {
        self.table_engine = engine.into();
        self.table_engine_params = Some(params.into());
        self
    }

    /// Sets a TTL duration for the spans table.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    // Optional: If implementing flattened attributes table
    // pub fn with_attributes_table(mut self, name: impl Into<String>) -> Self {
    //     self.attributes_table_name = name.into();
    //     self
    // }

    /// If set to `true`, the exporter will attempt to create the necessary
    /// ClickHouse tables (`CREATE TABLE IF NOT EXISTS`) upon initialization.
    /// Defaults to `false`.
    pub fn with_schema_creation(mut self, create: bool) -> Self {
        self.create_schema = create;
        self
    }

    /// Returns the ON CLUSTER clause or empty if none.
    pub fn cluster_string(&self) -> String {
        if let Some(ref c) = self.cluster_name {
            format!("ON CLUSTER {}", c)
        } else {
            String::new()
        }
    }

    /// Returns the full table engine string (e.g. `MergeTree()` or `ModEngine(param)`).
    pub fn table_engine_string(&self) -> String {
        if let Some(ref params) = self.table_engine_params {
            format!("{}({})", self.table_engine, params)
        } else {
            format!("{}()", self.table_engine)
        }
    }

    /// Generates a TTL expression or empty string if no TTL is set.
    pub fn ttl_expr(&self, ts_expr: &str) -> String {
        if let Some(ttl) = self.ttl {
            format!("TTL {} + INTERVAL {} SECOND", ts_expr, ttl.as_secs())
        } else {
            String::new()
        }
    }
}
