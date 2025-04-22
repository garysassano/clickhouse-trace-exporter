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
use std::error::Error; // Import Error trait for boxing

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
    /// Update lifetime to 'static for BoxFuture
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        // Clone necessary data for the 'static future
        let pool = self.pool.clone(); // Pool is likely cheap to clone (Arc internally)
        let spans_table_name = self.config.spans_table_name.clone();
        let errors_table_name = self.config.errors_table_name.clone();

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }

            let batch_size = batch.len();
            tracing::debug!("Exporting batch of {} spans to ClickHouse", batch_size);

            // Note: get_resource_from_batch needs to be adjusted or called differently
            // as self is not available directly here. For now, using default.
            // This needs refinement - ideally pass Resource from provider.
            let resource = Cow::Owned(Resource::default());

            let mut span_rows: Vec<SpanRow> = Vec::with_capacity(batch_size);
            let mut error_rows: Vec<ErrorRow> = Vec::with_capacity(batch_size);

            for span_data in batch {
                let (span_row, mut errors) = convert_otel_span_to_rows(&span_data, &resource);
                span_rows.push(span_row);
                error_rows.append(&mut errors);
            }

            // Use the cloned pool
            let mut client = match pool.get_handle().await {
                Ok(client) => client,
                Err(e) => {
                    tracing::error!("Failed to get ClickHouse client handle from pool: {}", e);
                    // Box and cast the error for ExportResult
                    return Err(Box::new(ClickhouseExporterError::ClickhousePoolError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            };

            if !span_rows.is_empty() {
                // Use cloned table name
                let insert_result = client.insert(&spans_table_name)?.write(&span_rows).await;
                if let Err(e) = insert_result {
                    tracing::error!(
                        "Failed to insert spans into ClickHouse table '{}': {}",
                        spans_table_name,
                        e
                    );
                    // Box and cast the error for ExportResult
                    return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            }

            if !error_rows.is_empty() {
                 // Use cloned table name
                let insert_result = client.insert(&errors_table_name)?.write(&error_rows).await;
                if let Err(e) = insert_result {
                    tracing::warn!(
                        "Failed to insert errors into ClickHouse table '{}': {}",
                        errors_table_name,
                        e
                    );
                    // Optionally return error here too:
                    // return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>);
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
