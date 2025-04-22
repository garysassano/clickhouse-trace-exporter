use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{ // Import only necessary utils and helpers
    system_time_to_utc, attributes_to_map, duration_to_nanos, 
    span_kind_to_string, status_code_to_string, get_service_name,
    convert_events, convert_links
};
use crate::schema;
use async_trait::async_trait;
use clickhouse::{Client, ClientOptions}; // Use Client and ClientOptions
use opentelemetry::trace::Status;
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use futures::future::{BoxFuture, FutureExt};
use std::error::Error;

// SQL Templates (similar to Go version)
// Adapt column names and types based on schema.rs if they differ from Go version
const INSERT_TRACES_SQL: &str = r#"
INSERT INTO {table_name} (
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ServiceName,
    ResourceAttributes,
    ScopeName,
    ScopeVersion,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage,
    Events.Timestamp,
    Events.Name,
    Events.Attributes,
    Links.TraceId,
    Links.SpanId,
    Links.TraceState,
    Links.Attributes
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
    ?, ?
)
"#;


#[derive(Debug)]
pub struct ClickhouseExporter {
    client: Client, // Use Client
    config: ClickhouseExporterConfig,
    insert_sql: String, // Store rendered insert SQL
}

impl ClickhouseExporter {
    pub async fn new(config: ClickhouseExporterConfig) -> Result<Self, ClickhouseExporterError> {
        tracing::info!(
            "Initializing ClickHouse exporter (Host: {}, Schema Create: {})",
            config.dsn.host_str().unwrap_or("N/A"),
            config.create_schema
        );

        // Use Client::new and potentially ClientOptions if needed
        // Basic client from DSN URL string
        let client = Client::new(config.dsn.as_str());
        // Add options if necessary, e.g. for credentials
        // let opts = ClientOptions::default().with_user(...).with_password(...);
        // let client = Client::new(config.dsn.as_str()).with_options(opts);
        
        // Ping to verify connection? Client::ping doesn't seem async in v0.13?
        // Handle credentials if part of DSN or separate config fields

        tracing::debug!("Created ClickHouse client.");

        if config.create_schema {
            schema::ensure_schema(&client, &config).await?;
        }

        // Render the INSERT SQL template with the actual table name
        let insert_sql = INSERT_TRACES_SQL.replace("{table_name}", &config.spans_table_name);

        Ok(ClickhouseExporter { client, config, insert_sql })
    }
}

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        // Clone data needed for the static future
        let client = self.client.clone(); // Client is likely cheap to clone (Arc internally)
        let insert_sql = self.insert_sql.clone();

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }

            // Note: The Go exporter uses a transaction per batch. 
            // The Rust clickhouse v0.13 client might not have explicit transaction support 
            // in the same way as database/sql. Inserts might be atomic per statement.
            // Revisit if transactions are strictly needed.

            for span_data in batch {
                let resource = &span_data.resource; // Resource is Cow, borrow it
                let scope = &span_data.instrumentation_lib;

                let service_name = get_service_name(resource);
                let resource_attrs = attributes_to_map(resource.iter().map(|(k,v)| KeyValue::new(k.clone(), v.clone())).collect::<Vec<_>>().iter());
                let span_attrs = attributes_to_map(&span_data.attributes);
                
                let (event_times, event_names, event_attrs) = convert_events(&span_data.events);
                let (link_trace_ids, link_span_ids, link_trace_states, link_attrs) = convert_links(&span_data.links);

                let status_message = match &span_data.status {
                    Status::Error { description } => description.to_string(),
                    _ => "".to_string(),
                };

                let duration_ns = span_data
                    .end_time
                    .duration_since(span_data.start_time)
                    .map(duration_to_nanos)
                    .unwrap_or(0);

                // Execute INSERT for each span
                // Parameter order must exactly match the VALUES clause placeholders
                let result = client.execute_with_params(
                    &insert_sql,
                    params!{
                        // Match param order with INSERT_TRACES_SQL
                        "Timestamp" => system_time_to_utc(span_data.start_time),
                        "TraceId" => span_data.span_context.trace_id().to_string(),
                        "SpanId" => span_data.span_context.span_id().to_string(),
                        "ParentSpanId" => span_data.parent_span_id.to_string(),
                        "TraceState" => span_data.span_context.trace_state().header().to_string(),
                        "SpanName" => span_data.name.to_string(),
                        "SpanKind" => span_kind_to_string(&span_data.span_kind),
                        "ServiceName" => service_name,
                        "ResourceAttributes" => resource_attrs,
                        "ScopeName" => scope.name.to_string(),
                        "ScopeVersion" => scope.version.map_or("".to_string(), |v| v.to_string()),
                        "SpanAttributes" => span_attrs,
                        "Duration" => duration_ns, // Send as i64
                        "StatusCode" => status_code_to_string(&span_data.status),
                        "StatusMessage" => status_message,
                        "Events.Timestamp" => event_times,
                        "Events.Name" => event_names,
                        "Events.Attributes" => event_attrs,
                        "Links.TraceId" => link_trace_ids,
                        "Links.SpanId" => link_span_ids,
                        "Links.TraceState" => link_trace_states,
                        "Links.Attributes" => link_attrs,
                    }
                ).await;

                if let Err(e) = result {
                    tracing::error!(
                        "Failed to insert span {} into ClickHouse: {}", 
                        span_data.span_context.span_id(),
                        e
                    );
                    // Decide on batch error handling: return on first error?
                    return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            }

            tracing::debug!("Successfully inserted {} spans.", batch_size); // Removed batch_size log previously
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
        // Client drop should handle cleanup
    }
}
