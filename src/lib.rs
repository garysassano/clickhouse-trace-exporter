//! An OpenTelemetry trace exporter implementation for ClickHouse using the Rust SDK.
//!
//! This crate provides `ClickhouseExporter` which implements the `SpanExporter`
//! trait from the `opentelemetry-sdk` crate. It allows sending OpenTelemetry
//! trace data to a ClickHouse database.
//!
//! # Structure
//!
//! - `ClickhouseExporterConfig`: Configuration for the exporter (DSN, table names, etc.).
//! - `ClickhouseExporter`: The main exporter struct implementing `SpanExporter`.
//! - `ClickhouseExporterError`: Custom error type for the exporter.
//!
//! # Usage
//!
//! ```no_run
//! use opentelemetry_sdk::{trace as sdktrace, runtime, Resource};
//! use opentelemetry::{global, trace::{Tracer, TracerProvider as _, TraceError}, KeyValue};
//! use opentelemetry_clickhouse_exporter::{ClickhouseExporter, ClickhouseExporterConfig}; // Use crate name
//! use std::env;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Example: Load DSN from environment variable
//!     let clickhouse_dsn = env::var("CLICKHOUSE_DSN")
//!         .unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".to_string());
//!
//!     // Configure the exporter
//!     let exporter_config = ClickhouseExporterConfig::new(clickhouse_dsn)?
//!         .with_schema_creation(true); // Create tables if they don't exist
//!
//!     // Create the exporter asynchronously
//!     let exporter = ClickhouseExporter::new(exporter_config).await?;
//!
//!     // Build a tracer provider with a batch processor
//!     let provider = sdktrace::TracerProvider::builder()
//!         .with_batch_exporter(exporter, runtime::Tokio)
//!         .with_config(sdktrace::config().with_resource(Resource::new(vec![
//!             KeyValue::new("service.name", "my-app-service"),
//!         ])))
//!         .build();
//!
//!     global::set_tracer_provider(provider);
//!
//!     // Use the global tracer
//!     let tracer = global::tracer("my-app");
//!     tracer.in_span("main operation", |cx| {
//!         // ... your application logic ...
//!         tracer.in_span("sub operation", |_cx| {
//!              // ...
//!         });
//!     });
//!
//!     // Shut down the provider to flush traces
//!     global::shutdown_tracer_provider();
//!
//!     Ok(())
//! }
//! ```

mod config;
mod error;
mod exporter;
mod model;
mod schema;

pub use config::ClickhouseExporterConfig;
pub use error::ClickhouseExporterError;
pub use exporter::ClickhouseExporter;
