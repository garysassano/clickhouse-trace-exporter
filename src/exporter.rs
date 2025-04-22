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

    // Helper to get resource from SpanData batch (assuming it's consistent)
    fn get_resource_from_batch<'a>(&self, batch: &'a [SpanData]) -> Cow<'a, Resource> {
        batch
            .first()
            .map(|sd| sd.resource.clone()) // sd.resource is likely Cow<Resource>
            .unwrap_or_else(|| {
                // Fallback if batch is empty: return a default owned resource.
                // Consider logging a warning here.
                Cow::Owned(Resource::default()) // Or Resource::empty()
            })
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    /// Exports a batch of spans.
    /// Updated signature to return BoxFuture and match trait bounds
    fn export<'a>(&'a mut self, batch: Cow<'a, [SpanData]>) -> BoxFuture<'a, ExportResult> {
        // Wrap the async logic in Box::pin
        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }

            let batch_size = batch.len();
            tracing::debug!("Exporting batch of {} spans to ClickHouse", batch_size);

            let resource = self.get_resource_from_batch(&batch);

            let mut span_rows: Vec<SpanRow> = Vec::with_capacity(batch_size);
            let mut error_rows: Vec<ErrorRow> = Vec::with_capacity(batch_size);

            for span_data in batch.iter() {
                let (span_row, mut errors) = convert_otel_span_to_rows(span_data, &resource);
                span_rows.push(span_row);
                error_rows.append(&mut errors);
            }

            let mut client = match self.pool.get_handle().await {
                Ok(client) => client,
                Err(e) => {
                    tracing::error!("Failed to get ClickHouse client handle from pool: {}", e);
                    return Err(ClickhouseExporterError::ClickhousePoolError(e).into());
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
                    return Err(ClickhouseExporterError::ClickhouseClientError(e).into());
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
                    // Decide if this should be a hard failure for the whole batch export
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
