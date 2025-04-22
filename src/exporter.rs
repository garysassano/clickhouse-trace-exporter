use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{attributes_to_map, ErrorRow, EventRow, LinkRow, SpanRow, convert_otel_span_to_rows};
use crate::schema;
use async_trait::async_trait;
use clickhouse::Client; // Use correct crate name
use opentelemetry::trace::TraceError;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::resource::ResourceDetector;
use std::sync::Arc;
use std::time::Duration;

/// Exports OpenTelemetry spans to ClickHouse.
#[derive(Debug)]
pub struct ClickhouseExporter {
    client: Client,
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

        // Consider adding connection timeout options if needed via `Options` builder
        let options = Options::from_url(config.dsn.clone())?;

        // Connect to ClickHouse
        // Add retry logic here if needed for initial connection
        let client = Client::connect(options).await?;
        tracing::debug!("Successfully connected to ClickHouse.");

        // Ensure schema exists if requested (run before exporting)
        schema::ensure_schema(&client, &config).await?;

        Ok(ClickhouseExporter { client, config })
    }

    // Helper to get resource from SpanData batch (assuming it's consistent)
    // Alternatively, pass Resource during `new` if the SDK allows.
    fn get_resource_from_batch(&self, batch: &[SpanData]) -> Arc<Resource> {
        batch
            .first()
            .map(|sd| sd.resource.clone())
            .unwrap_or_else(|| {
                // Fallback if batch is empty or resource isn't attached (shouldn't happen with SDK)
                Arc::new(Resource::from_detectors(
                    Duration::from_secs(0),
                    vec![Box::new(
                        opentelemetry_sdk::resource::EnvResourceDetector::new(),
                    )], // Example basic detector
                ))
            })
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    /// Exports a batch of spans.
    ///
    /// This method converts the `SpanData` into ClickHouse row structures
    /// and inserts them into the configured tables using batch `INSERT`.
    async fn export(&mut self, batch: Vec<SpanData>) -> ExportResult {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        tracing::debug!("Exporting batch of {} spans to ClickHouse", batch_size);

        // Extract resource (assuming it's consistent for the batch)
        let resource = self.get_resource_from_batch(&batch);

        // Prepare data structures for insertion
        let mut span_rows: Vec<SpanRow> = Vec::with_capacity(batch_size);
        let mut error_rows: Vec<ErrorRow> = Vec::with_capacity(batch_size); // Capacity might be too high

        for span_data in batch {
            // Convert each span, passing the extracted resource
            let (span_row, mut errors) = convert_otel_span_to_rows(&span_data, &resource);
            span_rows.push(span_row);
            error_rows.append(&mut errors);
            // Add logic here for flattened attributes if implemented
        }

        // --- Insert Spans ---
        if !span_rows.is_empty() {
            let table_name = self.config.spans_table_name.clone();
            tracing::trace!(
                "Inserting {} rows into spans table: {}",
                span_rows.len(),
                &table_name
            );
            let insert_result = self.client.insert(&table_name, span_rows).await; // `insert` takes ownership
            if let Err(e) = insert_result {
                tracing::error!(
                    "Failed to insert spans into ClickHouse table '{}': {}",
                    table_name,
                    e
                );
                // Map the error to ExportResult::Err
                return Err(ClickhouseExporterError::ClickhouseClientError(e).into());
            }
            tracing::debug!("Successfully inserted {} span rows.", batch_size); // Use original batch size for log
        }

        // --- Insert Errors ---
        if !error_rows.is_empty() {
            let num_errors = error_rows.len();
            let table_name = self.config.errors_table_name.clone();
            tracing::trace!(
                "Inserting {} rows into errors table: {}",
                num_errors,
                &table_name
            );
            let insert_result = self.client.insert(&table_name, error_rows).await;
            if let Err(e) = insert_result {
                // Failing to insert errors might be less critical than spans, log warning or error?
                tracing::warn!(
                    "Failed to insert errors into ClickHouse table '{}': {}",
                    table_name,
                    e
                );
                // Decide if this should be a hard failure for the whole batch export
                // return Err(ClickhouseExporterError::ClickhouseClientError(e).into()); // Option: Fail batch
            } else {
                tracing::debug!("Successfully inserted {} error rows.", num_errors);
            }
        }

        // --- Insert Attributes (if using flattened table) ---
        // if !attribute_rows.is_empty() { ... }

        Ok(()) // Indicate successful export of the batch
    }

    /// Called by the SDK when shutting down.
    async fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
        // The `clickhouse` client might handle pool closure automatically on drop.
        // Explicit cleanup (like waiting for ongoing inserts) might be needed
        // if the exporter itself managed complex buffering or async tasks.
        // For this basic version, likely no action is strictly required here.
    }
}
