use thiserror::Error;

// Use correct crate name: clickhouse
use clickhouse::error::Error as ClickhouseError;

#[derive(Error, Debug)]
pub enum ClickhouseExporterError {
    #[error("Failed to parse ClickHouse DSN: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("ClickHouse client error: {0}")]
    ClickhouseClientError(ClickhouseError),

    #[error("Schema creation failed: {0}")]
    SchemaCreationError(ClickhouseError),

    #[error("Data conversion error: {0}")]
    ConversionError(String),

    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),
}
