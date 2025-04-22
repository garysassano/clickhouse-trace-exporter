use opentelemetry::trace::TraceError;
use opentelemetry_sdk::export::ExportError;
use thiserror::Error;

// Use correct crate name: clickhouse
use clickhouse::error::Error as ClickhouseError;

#[derive(Error, Debug)]
pub enum ClickhouseExporterError {
    #[error("Failed to parse ClickHouse DSN: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("ClickHouse client error: {0}")]
    ClickhouseClientError(ClickhouseError),

    #[error("ClickHouse pool error: {0}")]
    ClickhousePoolError(#[from] ClickhouseError),

    #[error("Schema creation failed: {0}")]
    SchemaCreationError(ClickhouseError),

    #[error("Data conversion error: {0}")]
    ConversionError(String),

    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),
}

// Convert internal errors to OTel ExportError
impl From<ClickhouseExporterError> for ExportError {
    fn from(e: ClickhouseExporterError) -> Self {
        ExportError::other(e.to_string())
    }
}

// Convert internal errors to OTel TraceError
impl From<ClickhouseExporterError> for TraceError {
    fn from(e: ClickhouseExporterError) -> Self {
        TraceError::ExportError(ExportError::other(e.to_string()))
    }
}
