use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{ // Import necessary utils and helpers
    system_time_to_utc, attributes_to_map, duration_to_nanos, 
    span_kind_to_string, status_code_to_string, get_service_name,
    convert_events, convert_links
};
use crate::schema;
use async_trait::async_trait;
use clickhouse::Client;
use opentelemetry::{
    trace::Status,
    KeyValue
};
use opentelemetry_sdk::{
    export::trace::{ExportResult, SpanData, SpanExporter},
};
use futures::future::{BoxFuture};
use std::error::Error;
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use std::fmt; // Import fmt for manual Debug impl

// Define a struct that derives `Row` for insertion
// Ensure field names match the SQL insert columns exactly
#[derive(clickhouse::Row, Clone, serde::Serialize)] // Removed Debug derive, Added Clone
struct SpanRow { // Removed lifetime 'a
    #[serde(rename = "Timestamp")] timestamp: DateTime<Utc>,
    #[serde(rename = "TraceId")] trace_id: String, // Changed to owned String
    #[serde(rename = "SpanId")] span_id: String,   // Changed to owned String
    #[serde(rename = "ParentSpanId")] parent_span_id: String,
    #[serde(rename = "TraceState")] trace_state: String,
    #[serde(rename = "SpanName")] span_name: String,
    #[serde(rename = "SpanKind")] span_kind: String,
    #[serde(rename = "ServiceName")] service_name: String,
    #[serde(rename = "ResourceAttributes")] resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeName")] scope_name: String,
    #[serde(rename = "ScopeVersion")] scope_version: String,
    #[serde(rename = "SpanAttributes")] span_attributes: HashMap<String, String>,
    #[serde(rename = "Duration")] duration: u64,
    #[serde(rename = "StatusCode")] status_code: String,
    #[serde(rename = "StatusMessage")] status_message: String,
    #[serde(rename = "Events")] events: Vec<crate::schema::EventRow>,
    #[serde(rename = "Links")] links: Vec<crate::schema::LinkRow>,
}

// Add manual Debug impl because clickhouse::Client doesn't implement it
pub struct ClickhouseExporter {
    // Client is likely not thread-safe for cloning and using across async tasks
    // depending on the underlying HTTP client used by clickhouse-rs.
    // Consider Arc<Client> or a connection pool (like bb8/deadpool) for robustness.
    // For now, cloning works with default reqwest client, but beware.
    client: Client,
    config: ClickhouseExporterConfig,
}

// Manual Debug impl
impl fmt::Debug for ClickhouseExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseExporter")
         .field("client", &"<Clickhouse Client>") // Placeholder for non-Debug client
         .field("config", &self.config)
         .finish()
    }
}

impl ClickhouseExporter {
    pub async fn new(config: ClickhouseExporterConfig) -> Result<Self, ClickhouseExporterError> {
        tracing::info!(
            "Initializing ClickHouse exporter (Host: {}, Schema Create: {})",
            config.dsn.host_str().unwrap_or("N/A"),
            config.create_schema
        );

        // Create client using default and configuring with URL (suitable for clickhouse v0.13.x)
        let client = Client::default().with_url(config.dsn.as_str());

        // Verify connection by executing a simple query
        client.query("SELECT 1").execute().await.map_err(ClickhouseExporterError::ClickhouseClientError)?;
        tracing::debug!("Successfully connected to ClickHouse.");

        if config.create_schema {
            schema::ensure_schema(&client, &config).await?;
        }

        Ok(ClickhouseExporter { client, config })
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        // Clone client for the async block.
        // WARNING: See note on ClickhouseExporter struct about client cloning.
        let client = self.client.clone();
        let table_name = self.config.spans_table_name.clone();

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }
            let batch_size = batch.len(); // For logging
            let start_time = std::time::Instant::now(); // For timing

            // Use `client.insert` for batch insertion which handles transactions implicitly.
            let mut insert = client.insert(&table_name).map_err(|e| {
                Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>
            })?;

            for span_data in &batch { // Borrow batch
                let resource = &span_data.resource;
                let scope = &span_data.instrumentation_lib;

                let service_name = get_service_name(resource);
                let resource_attrs = attributes_to_map(resource.iter().map(|(k, v)| KeyValue::new(k.clone(), v.clone())).collect::<Vec<_>>().iter());
                let span_attrs = attributes_to_map(&span_data.attributes);

                // Use updated model functions for Nested types
                let events = convert_events(&span_data.events);
                let links = convert_links(&span_data.links);

                let status_message = match &span_data.status {
                    Status::Error { description } => description.to_string(),
                    _ => "".to_string(),
                };

                let duration_ns = span_data
                    .end_time
                    .duration_since(span_data.start_time)
                    .map(duration_to_nanos)
                    .unwrap_or(0);

                // Construct the row struct for insertion
                let row = SpanRow {
                    timestamp: system_time_to_utc(span_data.start_time),
                    trace_id: span_data.span_context.trace_id().to_string(), // Move owned String
                    span_id: span_data.span_context.span_id().to_string(),   // Move owned String
                    parent_span_id: span_data.parent_span_id.to_string(),
                    trace_state: span_data.span_context.trace_state().header().to_string(),
                    span_name: span_data.name.to_string(), // Convert Cow -> String
                    span_kind: span_kind_to_string(&span_data.span_kind),
                    service_name,
                    resource_attributes: resource_attrs,
                    scope_name: scope.name.to_string(),
                    scope_version: scope.version.as_deref().map_or("", |v| v).to_string(),
                    span_attributes: span_attrs,
                    duration: duration_ns,
                    status_code: status_code_to_string(&span_data.status),
                    status_message,
                    events,
                    links,
                };

                // Write the row to the insert batch
                insert.write(&row).await.map_err(|e| {
                    tracing::error!(
                        "Failed preparing span {} for ClickHouse batch insert: {}",
                        row.span_id, // Use row.span_id now it's owned
                        e
                    );
                    Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>
                })?;
            }

            // Finalize the insert operation (sends the batch)
            insert.end().await.map_err(|e| {
                tracing::error!("Failed executing ClickHouse batch insert: {}", e);
                Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>
            })?;

            let elapsed = start_time.elapsed();
            tracing::debug!(
                "Successfully inserted {} spans into ClickHouse in {:.2?}",
                batch_size,
                elapsed
            );
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
    }
}
