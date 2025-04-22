use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClickhouseExporterError {
    #[error("Failed to parse ClickHouse DSN: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("ClickHouse client error: {0}")]
    ClickhouseClientError(#[from] clickhouse::error::Error), // Use correct crate name

    #[error("Schema creation failed: {0}")]
    SchemaCreationError(clickhouse::error::Error), // Use correct crate name

    #[error("Data conversion error: {0}")]
    ConversionError(String),

    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),
}

// Helper to convert internal errors to OTel ExportError
impl From<ClickhouseExporterError> for opentelemetry_sdk::export::trace::ExportError {
    fn from(e: ClickhouseExporterError) -> Self {
        // Consider more specific mappings if needed (e.g., transient vs permanent errors)
        opentelemetry_sdk::export::trace::ExportError::other(e.to_string())
    }
}
