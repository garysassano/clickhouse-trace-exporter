use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{ // Import necessary utils and helpers
    system_time_to_utc, attributes_to_map, duration_to_nanos, 
    span_kind_to_string, status_code_to_string, get_service_name,
    convert_events, convert_links
};
use crate::schema;
use async_trait::async_trait;
use clickhouse::{Client, ClientOptions, Row}; // Use Client and ClientOptions. Row needed for params macro.
use opentelemetry::{
    trace::{Status, SpanKind, Link}, 
    KeyValue, Value
};
use opentelemetry_sdk::{
    export::trace::{ExportResult, SpanData, SpanExporter},
    Resource
};
use futures::future::{BoxFuture};
use std::error::Error;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

// SQL Template for insertion
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
    `Events.Timestamp`,
    `Events.Name`,
    `Events.Attributes`,
    `Links.TraceId`,
    `Links.SpanId`,
    `Links.TraceState`,
    `Links.Attributes`
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
        let options = ClientOptions::default(); // Add options like user/pass if needed
        let client = Client::new(config.dsn.as_str());
        // Handle authentication if configured
        let client = if let Some(user) = &config.username {
            client.with_user(user.clone())
        } else { client };
        let client = if let Some(pass) = &config.password {
            client.with_password(pass.clone())
        } else { client };
        // Apply other options from config or DSN?
        // let client = client.with_options(options);

        // Ping to verify connection?
        client.ping().await?; // Assuming ping exists and is async
        tracing::debug!("Successfully connected to ClickHouse.");

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
        let client = self.client.clone();
        let insert_sql = self.insert_sql.clone();

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }
            let batch_size = batch.len(); // For logging

            // Note: The Go exporter uses a transaction per batch.
            // clickhouse crate v0.13 doesn't expose simple transaction begin/commit.
            // Inserts will be per-span.

            for span_data in batch {
                let resource = &span_data.resource;
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

                // Construct a Row dynamically for insertion
                // Order MUST match VALUES (?, ...) placeholders
                let row = Row!{
                    system_time_to_utc(span_data.start_time),                  // Timestamp
                    span_data.span_context.trace_id().to_string(),            // TraceId
                    span_data.span_context.span_id().to_string(),             // SpanId
                    span_data.parent_span_id.to_string(),                     // ParentSpanId
                    span_data.span_context.trace_state().header().to_string(), // TraceState
                    span_data.name.to_string(),                               // SpanName
                    span_kind_to_string(&span_data.span_kind),                // SpanKind
                    service_name,                                             // ServiceName
                    resource_attrs,                                           // ResourceAttributes (HashMap)
                    scope.name.to_string(),                                   // ScopeName
                    scope.version.map_or("".to_string(), |v| v.to_string()),  // ScopeVersion
                    span_attrs,                                               // SpanAttributes (HashMap)
                    duration_ns,                                              // Duration (i64)
                    status_code_to_string(&span_data.status),                 // StatusCode
                    status_message,                                           // StatusMessage
                    event_times,                                              // Events.Timestamp (Vec<DateTime>)
                    event_names,                                              // Events.Name (Vec<String>)
                    event_attrs,                                              // Events.Attributes (Vec<HashMap>)
                    link_trace_ids,                                           // Links.TraceId (Vec<String>)
                    link_span_ids,                                            // Links.SpanId (Vec<String>)
                    link_trace_states,                                        // Links.TraceState (Vec<String>)
                    link_attrs                                                // Links.Attributes (Vec<HashMap>)
                };

                // Execute INSERT for each span using execute (not insert+write)
                let result = client.execute(&insert_sql, row).await;

                if let Err(e) = result {
                    tracing::error!(
                        "Failed to insert span {} into ClickHouse: {}",
                        span_data.span_context.span_id(),
                        e
                    );
                    return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            }

            tracing::debug!("Successfully processed {} spans for ClickHouse.", batch_size);
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
    }
}
