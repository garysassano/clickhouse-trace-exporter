use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{
    attributes_to_vec,
    convert_events,
    convert_links,
    duration_to_nanos,
    span_kind_to_string,
    status_code_to_string, // Removed get_service_name
    // Import necessary utils and helpers
    system_time_to_utc,
};
use crate::schema;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use clickhouse::Compression;
use opentelemetry::{
    // Removed unused trace::Status
    Key, // Import Key for resource lookup
};
use opentelemetry_sdk::{
    Resource, // Keep Resource import for service name extraction attempt
    error::{OTelSdkError, OTelSdkResult}, // Fix: using correct imports
    // Import the correct SDK error types
    trace::{SpanData, SpanExporter},
};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME; // Import SERVICE_NAME
use std::fmt; // Import fmt for manual Debug impl

// Define a struct that derives `Row` for insertion
// Ensure field names match the SQL insert columns exactly
#[derive(clickhouse::Row, Clone, serde::Serialize)]
struct SpanRow {
    // Removed lifetime 'a
    #[serde(rename = "Timestamp")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "TraceId")]
    trace_id: String, // Changed to owned String
    #[serde(rename = "SpanId")]
    span_id: String, // Changed to owned String
    #[serde(rename = "ParentSpanId")]
    parent_span_id: String,
    #[serde(rename = "TraceState")]
    trace_state: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "SpanKind")]
    span_kind: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "SpanAttributes")]
    span_attributes: Vec<(String, String)>,
    #[serde(rename = "Duration")]
    duration: u64,
    #[serde(rename = "StatusCode")]
    status_code: String,
    #[serde(rename = "StatusMessage")]
    status_message: String,

    // Uncomment Nested fields and revert attribute types
    #[serde(rename = "Events.Timestamp")]
    events_timestamp: Vec<DateTime<Utc>>,
    #[serde(rename = "Events.Name")]
    events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    events_attributes: Vec<Vec<(String, String)>>,

    #[serde(rename = "Links.TraceId")]
    links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    links_attributes: Vec<Vec<(String, String)>>,
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
        resource: Option<Resource>, // Accept optional Resource
    ) -> Result<Self, ClickhouseExporterError> {
        tracing::info!(
            "Initializing ClickHouse exporter (Host: {}, Schema Create: {})",
            config.dsn.host_str().unwrap_or("N/A"),
            config.create_schema
        );

        // Build base HTTP URL without query (host, port, path)
        let mut base_url = config.dsn.clone();
        base_url.set_query(None);
        let mut client = Client::default().with_url(base_url.as_str());
        // Extract credentials from userinfo
        if !config.dsn.username().is_empty() {
            client = client.with_option("user", config.dsn.username());
        }
        if let Some(pass) = config.dsn.password() {
            client = client.with_option("password", pass);
        }
        // Parse query pairs for compression and secure and other options
        for (key, value) in config.dsn.query_pairs() {
            match key.as_ref() {
                "compression" => {
                    if value == "lz4" {
                        client = client.with_compression(Compression::Lz4);
                    }
                    // Add other compression types if needed
                }
                // REMOVE this "secure" match arm entirely.
                // "secure" => {
                //     if value == "true" {
                //         // This is incorrect for HTTP/HTTPS, TLS is based on URL scheme
                //         // client = client.with_option("secure", "true"); // REMOVE THIS LINE
                //     }
                // }
                _ => {
                    // Pass through any other options (like database, timeouts etc. if added)
                    client = client.with_option(key.as_ref(), value.as_ref());
                }
            }
        }

        // Verify connection by executing a simple query
        client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(ClickhouseExporterError::ClickhouseClientError)?;
        tracing::debug!("Successfully connected to ClickHouse.");

        if config.create_schema {
            schema::ensure_schema(&client, &config).await?;
        }

        Ok(ClickhouseExporter {
            client,
            config,
            resource,
        })
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
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        tracing::info!(
            "---> export function called with batch size: {}",
            batch.len()
        );

        // Clone client for the async block.
        // WARNING: Cloning client might not be ideal for performance/safety.
        // Consider Arc<Client> or connection pooling.
        let client = self.client.clone();
        let table_name = self.config.spans_table_name.clone();
        let service_name = self.get_service_name_from_resource();

        Box::pin(async move {
            if batch.is_empty() {
                tracing::debug!("--> Export batch is empty, returning Ok.");
                return Ok(());
            }
            let batch_size = batch.len();
            let start_time = std::time::Instant::now();

            let insert_result = async {
                let mut insert = client
                    .insert(&table_name)
                    .map_err(|e| ClickhouseExporterError::ClickhouseClientError(e))?;

                for span_data in &batch {
                    let scope = &span_data.instrumentation_scope;

                    // Get attributes as Vec<(String, String)> first
                    let resource_attrs_vec: Vec<(String, String)> = Vec::new(); // Placeholder for resource attrs
                    let span_attrs_vec = attributes_to_vec(&span_data.attributes);

                    // Re-enable Nested processing
                    // Get structured Events and Links data
                    let events_data = convert_events(&span_data.events);
                    let links_data = convert_links(&span_data.links);

                    // --- Flatten Nested types --- Start ---
                    let mut events_timestamp = Vec::with_capacity(events_data.len());
                    let mut events_name = Vec::with_capacity(events_data.len());
                    let mut events_attributes = Vec::with_capacity(events_data.len()); // Now Vec<Vec<(String, String)>>
                    for event in events_data {
                        events_timestamp.push(event.timestamp);
                        events_name.push(event.name);
                        // Push the Vec<(String, String)> directly
                        events_attributes.push(event.attributes);
                    }

                    let mut links_trace_id = Vec::with_capacity(links_data.len());
                    let mut links_span_id = Vec::with_capacity(links_data.len());
                    let mut links_trace_state = Vec::with_capacity(links_data.len());
                    let mut links_attributes = Vec::with_capacity(links_data.len()); // Now Vec<Vec<(String, String)>>
                    for link in links_data {
                        links_trace_id.push(link.trace_id);
                        links_span_id.push(link.span_id);
                        links_trace_state.push(link.trace_state);
                        // Push the Vec<(String, String)> directly
                        links_attributes.push(link.attributes);
                    }
                    // --- Flatten Nested types --- End ----

                    let status_message = match &span_data.status {
                        opentelemetry::trace::Status::Error { description } => {
                            description.to_string()
                        }
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
                        resource_attributes: resource_attrs_vec,
                        scope_name: scope.name().to_string(),
                        scope_version: scope.version().as_deref().map_or("", |v| v).to_string(),
                        span_attributes: span_attrs_vec,
                        duration: duration_ns,
                        status_code: status_code_to_string(&span_data.status),
                        status_message,
                        events_timestamp,
                        events_name,
                        events_attributes,
                        links_trace_id,
                        links_span_id,
                        links_trace_state,
                        links_attributes,
                    };

                    // Check write result explicitly if needed, though panics are more likely here
                    insert.write(&row).await.map_err(|e| {
                        tracing::error!(
                            "Error during insert.write for span {}: {:?}",
                            row.span_id,
                            e
                        );
                        ClickhouseExporterError::ClickhouseClientError(e)
                    })?;
                }

                // Explicitly check end() result
                match insert.end().await {
                    Ok(_) => {
                        tracing::debug!("Insert finished successfully.");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("Error during insert.end: {:?}", e);
                        // Return the error wrapped in the custom error type
                        Err(ClickhouseExporterError::ClickhouseClientError(e))
                    }
                }
            }
            .await; // Await the inner async block result

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
                    // Log the specific error before converting
                    tracing::error!("Export failed with error: {:?}", e);
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
