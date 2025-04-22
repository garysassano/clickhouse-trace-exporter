use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{attributes_to_map, ErrorRow, EventRow, LinkRow, SpanRow, convert_otel_span_to_rows};
use crate::schema;
use async_trait::async_trait;
use clickhouse::Pool; // Ensure correct crate name
use opentelemetry::trace::TraceError;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::resource::ResourceDetector;
use std::sync::Arc;
use std::time::Duration;
use std::borrow::Cow; // Import Cow
use futures::future::{BoxFuture, FutureExt}; // Import BoxFuture and FutureExt

/// Exports OpenTelemetry spans to ClickHouse.
#[derive(Debug)] // Add Debug back
pub struct ClickhouseExporter {
    pool: Pool, // Changed from client
    config: ClickhouseExporterConfig,
    // Cache the resource detected by the SDK provider if possible, avoids re-extracting per batch.
    // This might require changes if the provider's resource can change dynamically,
    // but typically it's static for the lifetime of the exporter.
    // resource: Arc<Resource>, // Consider if needed/obtainable during init
}

impl ClickhouseExporter {
    /// Creates a new ClickHouse exporter.
    ///
    /// Connects to ClickHouse using the DSN from the config and ensures
    /// the schema exists if `config.create_schema` is enabled.
    pub async fn new(config: ClickhouseExporterConfig) -> Result<Self, ClickhouseExporterError> {
        tracing::info!(
            "Initializing ClickHouse exporter (Host: {}, Schema Create: {})",
            config.dsn.host_str().unwrap_or("N/A"), // Avoid logging full DSN potentially with credentials
            config.create_schema
        );

        // Create the connection pool
        let pool = Pool::new(config.dsn.as_str())?;
        tracing::debug!("Successfully created ClickHouse connection pool.");

        // Ensure schema exists if requested (run before exporting)
        schema::ensure_schema(&pool, &config).await?;

        Ok(ClickhouseExporter { pool, config })
    }

    // Helper to get resource from SpanData batch
    // Adjusted to work with Vec<SpanData>
    fn get_resource_from_batch<'a>(&self, batch: &'a Vec<SpanData>) -> Cow<'a, Resource> {
        batch
            .first()
            .map(|sd| sd.resource.clone())
            .unwrap_or_else(|| {
                Cow::Owned(Resource::default())
            })
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    /// Exports a batch of spans.
    /// Reverted signature to use Vec<SpanData>
    fn export<'a>(&'a mut self, batch: Vec<SpanData>) -> BoxFuture<'a, ExportResult> {
        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }

            let batch_size = batch.len();
            tracing::debug!("Exporting batch of {} spans to ClickHouse", batch_size);

            let resource = self.get_resource_from_batch(&batch); // Pass Vec ref

            let mut span_rows: Vec<SpanRow> = Vec::with_capacity(batch_size);
            let mut error_rows: Vec<ErrorRow> = Vec::with_capacity(batch_size);

            // Iterate over owned Vec<SpanData>
            for span_data in batch {
                let (span_row, mut errors) = convert_otel_span_to_rows(&span_data, &resource);
                span_rows.push(span_row);
                error_rows.append(&mut errors);
            }

            let mut client = match self.pool.get_handle().await {
                Ok(client) => client,
                Err(e) => {
                    tracing::error!("Failed to get ClickHouse client handle from pool: {}", e);
                    // Box the error for ExportResult
                    return Err(Box::new(ClickhouseExporterError::ClickhousePoolError(e)));
                }
            };

            if !span_rows.is_empty() {
                let table_name = self.config.spans_table_name.clone();
                let insert_result = client.insert(&table_name)?.write(&span_rows).await;
                if let Err(e) = insert_result {
                    tracing::error!(
                        "Failed to insert spans into ClickHouse table '{}': {}",
                        table_name,
                        e
                    );
                    // Box the error for ExportResult
                    return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)));
                }
            }

            if !error_rows.is_empty() {
                let table_name = self.config.errors_table_name.clone();
                let insert_result = client.insert(&table_name)?.write(&error_rows).await;
                if let Err(e) = insert_result {
                    tracing::warn!(
                        "Failed to insert errors into ClickHouse table '{}': {}",
                        table_name,
                        e
                    );
                    // Optionally return error here too:
                    // return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)));
                }
            }

            Ok(())
        })
    }

    /// Called by the SDK when shutting down.
    /// Updated signature to remove async
    fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
        // Pool closure is handled automatically on drop.
    }
}
