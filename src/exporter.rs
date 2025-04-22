use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{ // Import necessary utils and helpers
    system_time_to_utc, attributes_to_map, duration_to_nanos, 
    span_kind_to_string, status_code_to_string, // Removed get_service_name
    convert_events, convert_links
};
use crate::schema;
use async_trait::async_trait;
use clickhouse::Client;
use opentelemetry::{
    // Removed unused trace::Status
    Key, // Import Key for resource lookup
};
use opentelemetry_sdk::{
    // Import the correct SDK error types
    trace::{SpanData, SpanExporter},
    error::{OTelSdkError, OTelSdkResult}, // Fix: using correct imports
    Resource, // Keep Resource import for service name extraction attempt
};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME; // Import SERVICE_NAME
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
    // Store resource info (like service name) if available during construction
    // This is a workaround as SpanData doesn't provide it directly in v0.29
    resource: Option<Resource>, 
}

// Manual Debug impl
impl fmt::Debug for ClickhouseExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseExporter")
         .field("client", &"<Clickhouse Client>") // Placeholder for non-Debug client
         .field("config", &self.config)
         .field("resource", &self.resource) // Debug resource if present
         .finish()
    }
}

impl ClickhouseExporter {
    // Allow passing optional resource during construction
    pub async fn new_with_resource(
        config: ClickhouseExporterConfig,
        resource: Option<Resource> // Accept optional Resource
    ) -> Result<Self, ClickhouseExporterError> {
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

        Ok(ClickhouseExporter { client, config, resource })
    }

    // Convenience function without resource (uses default)
    pub async fn new(config: ClickhouseExporterConfig) -> Result<Self, ClickhouseExporterError> {
        Self::new_with_resource(config, None).await
    }

    // Helper to get service name from stored resource or default
    fn get_service_name_from_resource(&self) -> String {
        self.resource
            .as_ref()
            .and_then(|res| res.get(&Key::from(SERVICE_NAME))) // Borrow the Key
            .map(|v| crate::model::value_to_string(&v))
            .unwrap_or_else(|| "unknown_service".to_string())
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    // Use the correct signature that matches the trait
    fn export(
        &self, 
        batch: Vec<SpanData>
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        // Clone client for the async block.
        // WARNING: Cloning client might not be ideal for performance/safety.
        // Consider Arc<Client> or connection pooling.
        let client = self.client.clone();
        let table_name = self.config.spans_table_name.clone();
        let service_name = self.get_service_name_from_resource();

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }
            let batch_size = batch.len();
            let start_time = std::time::Instant::now();

            let insert_result = async {
                let mut insert = client.insert(&table_name).map_err(|e| {
                    ClickhouseExporterError::ClickhouseClientError(e)
                })?;

                for span_data in &batch {
                    let scope = &span_data.instrumentation_scope;

                    let resource_attrs = HashMap::new(); // Placeholder for resource attrs
                    let span_attrs = attributes_to_map(&span_data.attributes);

                    let events = convert_events(&span_data.events);
                    let links = convert_links(&span_data.links);

                    let status_message = match &span_data.status {
                        opentelemetry::trace::Status::Error { description } => description.to_string(),
                        _ => "".to_string(),
                    };

                    let duration_ns = span_data
                        .end_time
                        .duration_since(span_data.start_time)
                        .map(duration_to_nanos)
                        .unwrap_or(0);

                    let row = SpanRow {
                        timestamp: system_time_to_utc(span_data.start_time),
                        trace_id: span_data.span_context.trace_id().to_string(),
                        span_id: span_data.span_context.span_id().to_string(),
                        parent_span_id: span_data.parent_span_id.to_string(),
                        trace_state: span_data.span_context.trace_state().header().to_string(),
                        span_name: span_data.name.to_string(),
                        span_kind: span_kind_to_string(&span_data.span_kind),
                        service_name: service_name.clone(),
                        resource_attributes: resource_attrs,
                        scope_name: scope.name().to_string(), // Use name() method
                        scope_version: scope.version().as_deref().map_or("", |v| v).to_string(), // Use version() method
                        span_attributes: span_attrs,
                        duration: duration_ns,
                        status_code: status_code_to_string(&span_data.status),
                        status_message,
                        events,
                        links,
                    };

                    insert.write(&row).await.map_err(|e| {
                         tracing::error!("Failed preparing span {} for ClickHouse batch insert: {}", row.span_id, e);
                        ClickhouseExporterError::ClickhouseClientError(e)
                    })?;
                }

                insert.end().await.map_err(|e| {
                    tracing::error!("Failed executing ClickHouse batch insert: {}", e);
                    ClickhouseExporterError::ClickhouseClientError(e)
                })?;

                Ok::<(), ClickhouseExporterError>(())
            }.await;

            // Map internal Result to OTelSdkResult
            match insert_result {
                Ok(_) => {
                    let elapsed = start_time.elapsed();
                    tracing::debug!(
                        "Successfully inserted {} spans into ClickHouse in {:.2?}",
                        batch_size,
                        elapsed
                    );
                    Ok(())
                }
                Err(e) => {
                    // Convert internal error to OTelSdkError::Other
                    Err(OTelSdkError::InternalFailure(e.to_string()))
                }
            }
        })
    }

    // Use the correct OTelSdkResult type
    fn shutdown(&mut self) -> OTelSdkResult {
        tracing::info!("Shutting down ClickHouse exporter.");
        // Potentially add client shutdown logic if available/needed
        Ok(())
    }
}
