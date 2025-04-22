use opentelemetry::{
    KeyValue, global,
    trace::{Span, SpanKind, Status, TraceContextExt, Tracer, TracerProvider as _},
};
// Use the actual crate name here:
use opentelemetry_clickhouse_exporter::{ClickhouseExporter, ClickhouseExporterConfig};
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use std::{env, error::Error, time::Duration};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // Load .env file if available (useful for storing CLICKHOUSE_DSN)
    dotenvy::dotenv().ok(); // Use dotenvy instead of dotenv

    // Setup tracing subscriber for logging output
    tracing_subscriber::registry()
        .with(fmt::layer().pretty()) // Pretty print logs
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,opentelemetry_clickhouse_exporter=debug".into()),
        )
        .init();

    info!("Starting ClickHouse exporter example...");

    // --- Configuration ---
    let clickhouse_dsn = env::var("CLICKHOUSE_DSN")
        .map_err(|_| "Environment variable CLICKHOUSE_DSN is not set".to_string())?; // Fail if not set
    info!("Using ClickHouse DSN (host redacted)"); // Avoid logging full DSN in production

    let exporter_config = ClickhouseExporterConfig::new(clickhouse_dsn)?
        .with_schema_creation(true) // Ensure tables are created if they don't exist
        .with_spans_table("otel_spans_example") // Use specific table names for the example
        .with_errors_table("otel_span_errors_example");

    // --- Initialize Exporter and Tracer Provider ---
    info!("Initializing ClickHouse exporter...");
    let exporter = ClickhouseExporter::new(exporter_config).await?;
    info!("Exporter initialized.");

    let provider = sdktrace::TracerProvider::builder()
        // Use a batch processor for better performance
        .with_batch_exporter(exporter, runtime::Tokio)
        // Define application resource attributes
        .with_config(sdktrace::config().with_resource(Resource::new(vec![
            // Standard semantic conventions
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                "clickhouse-example-app",
            ),
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                "0.1.1",
            ),
            KeyValue::new("environment", "development"), // Custom attribute
        ])))
        .build();

    info!("TracerProvider created.");

    // Set this provider as the global default (optional, but common)
    let _ = global::set_tracer_provider(provider.clone()); // Clone provider if needed elsewhere

    // Get a tracer instance from the provider
    let tracer = global::tracer("example-tracer"); // Use global tracer

    // --- Create Spans ---
    info!("Creating trace data...");

    tracer.in_span("root-operation", |cx| {
        let span = cx.span();
        span.set_attribute(KeyValue::new("app.feature", "example_feature"));
        span.add_event("Starting root work", vec![]);
        info!("Inside root span...");

        // Simulate work
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Create a child span for a simulated database call
        tracer.in_span("child-db-query", |cx_child| {
            let child_span = cx_child.span();
            child_span.set_kind(SpanKind::Client);
            child_span.set_attribute(KeyValue::new(
                opentelemetry_semantic_conventions::trace::DB_SYSTEM,
                "clickhouse",
            ));
            child_span.set_attribute(KeyValue::new("db.operation", "SELECT"));
            info!("Inside child DB span...");

            tokio::time::sleep(Duration::from_millis(35)).await;

            // Simulate an error scenario
            let error_message = "Connection timeout to database";
            child_span.set_status(Status::error(error_message));
            child_span.record_error(&std::io::Error::new(
                // Use record_error helper
                std::io::ErrorKind::TimedOut,
                error_message,
            ));
            child_span.add_event(
                "Database Error Occurred", // Custom event name
                vec![
                    KeyValue::new("error.details", error_message),
                    KeyValue::new("db.query.id", "q-9876"),
                ],
            );
            info!("Child DB span finished with error.");
        }); // Child span ends

        span.add_event("Root work finished", vec![]);
        span.set_status(Status::Ok); // Set status for the root span
        info!("Root span finished.");
    }); // Root span ends

    info!("Trace data created. Spans should be queued for export.");

    // --- Shutdown ---
    // Shut down the provider to ensure all buffered spans are flushed.
    // This is crucial for batch processors.
    info!("Shutting down TracerProvider to flush spans...");
    global::shutdown_tracer_provider(); // Use global shutdown
    info!("Shutdown complete. Check ClickHouse.");

    Ok(())
}
