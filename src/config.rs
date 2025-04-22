use crate::error::ClickhouseExporterError;
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
}
