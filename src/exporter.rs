use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use crate::model::{ // Import necessary utils and helpers
    system_time_to_utc, attributes_to_map, duration_to_nanos, 
    span_kind_to_string, status_code_to_string, get_service_name,
    convert_events, convert_links
};
use crate::schema;
use async_trait::async_trait;
use clickhouse::{Client, ClientOptions, error::Error as ClickhouseError, query::Row}; // Use Client and ClientOptions, Error, Row for params macro?
use opentelemetry::{
    trace::{Status, SpanKind, Link}, 
    KeyValue, Value
};
use opentelemetry_sdk::{
    export::trace::{ExportResult, SpanData, SpanExporter},
    Resource
};
use futures::future::{BoxFuture, FutureExt};
use std::error::Error;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

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
    `Events.Timestamp`,
    `Events.Name`,
    `Events.Attributes`,
    `Links.TraceId`,
    `Links.SpanId`,
    `Links.TraceState`,
    `Links.Attributes`
) VALUES
"#;
// Note: We are not using VALUES (?,...) because execute_with_params is not available.
// We will use the row-based insert: client.insert(table)?.write(&block).await
// This means we NEED the Row structs back.

// ===== REVERTING MODEL CHANGES - NEED ROW STRUCTS =====
// The clickhouse client v0.13 does not seem to have an easy equivalent 
// for execute_with_params for arbitrary types/flattened arrays.
// The intended way seems to be using `client.insert(table)?.write(&block)`
// where block is a `Vec<YourRowStruct>`. 
// We need to go back to using the Row structs and derive.

// Placeholder: Keep existing exporter structure but acknowledge we need Row structs
// The following code WILL NOT COMPILE without bringing back Row structs and 
// reverting model.rs changes.


#[derive(Debug)]
pub struct ClickhouseExporter {
    // Let's try Pool again now other errors are fixed?
    // If Pool still fails, then Client approach needs more investigation
    // on how to insert without Row derive or with complex types.
    pool: clickhouse::Pool, 
    config: ClickhouseExporterConfig,
    // insert_sql: String, // Not needed for row-based insert
}

impl ClickhouseExporter {
    pub async fn new(config: ClickhouseExporterConfig) -> Result<Self, ClickhouseExporterError> {
        tracing::info!(
            "Initializing ClickHouse exporter (Host: {}, Schema Create: {})",
            config.dsn.host_str().unwrap_or("N/A"),
            config.create_schema
        );
        
        // Try Pool again
        let pool = clickhouse::Pool::new(config.dsn.as_str())?;
        tracing::debug!("Created ClickHouse connection pool.");

        if config.create_schema {
            schema::ensure_schema(&pool, &config).await?;
        }

        Ok(ClickhouseExporter { pool, config })
    }

     // Helper to get resource from SpanData batch
     fn get_resource_from_batch<'a>(&self, batch: &'a Vec<SpanData>) -> Cow<'a, Resource> {
        batch
            .first()
            .map(|sd| sd.resource.clone())
            .unwrap_or_else(|| {
                Cow::Owned(Resource::default())
            })
    }
}

// Bring back model imports
use crate::model::{SpanRow, ErrorRow, convert_otel_span_to_rows};
use std::borrow::Cow;

#[async_trait]
impl SpanExporter for ClickhouseExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let pool = self.pool.clone();
        let spans_table_name = self.config.spans_table_name.clone();
        // Need ErrorRow logic again if we separate errors
        // let errors_table_name = self.config.errors_table_name.clone(); 

        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }
            let batch_size = batch.len();

            let resource = batch.first().map(|sd| sd.resource.clone()).unwrap_or_else(|| Cow::Owned(Resource::default()));

            let mut span_rows: Vec<SpanRow> = Vec::with_capacity(batch_size);
            // let mut error_rows: Vec<ErrorRow> = Vec::with_capacity(batch_size);

            for span_data in batch {
                 // Assuming convert_otel_span_to_rows is back and works with Row derive
                let (span_row, _ /* mut errors */) = convert_otel_span_to_rows(&span_data, &resource);
                span_rows.push(span_row);
                // error_rows.append(&mut errors);
            }

            let mut client = match pool.get_handle().await {
                Ok(client) => client,
                Err(e) => {
                    return Err(Box::new(ClickhouseExporterError::ClickhousePoolError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            };

            if !span_rows.is_empty() {
                let insert_result = client.insert(&spans_table_name)?.write(&span_rows).await;
                if let Err(e) = insert_result {
                    return Err(Box::new(ClickhouseExporterError::ClickhouseClientError(e)) as Box<dyn Error + Send + Sync + 'static>);
                }
            }

            // Add back ErrorRow insertion if needed
            /* 
            if !error_rows.is_empty() {
                let insert_result = client.insert(&errors_table_name)?.write(&error_rows).await;
                if let Err(e) = insert_result {
                    // Log warning or return error?
                }
            }
            */

            tracing::debug!("Successfully inserted {} spans.", span_rows.len());
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        tracing::info!("Shutting down ClickHouse exporter.");
    }
}
