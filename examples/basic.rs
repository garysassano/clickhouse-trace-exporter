//! Basic exporter example
use opentelemetry::{
    global,
    trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider}, // Removed Span, Added TracerProvider
    KeyValue,
};
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
use std::env;
use std::time::Duration;
use opentelemetry_clickhouse_exporter::{ClickhouseExporter, ClickhouseExporterConfig, model::span_kind_to_string};

// Import attribute::* for semantic conventions
use opentelemetry_semantic_conventions::attribute;

fn init_tracer(config: ClickhouseExporterConfig) -> Result<sdktrace::Tracer, Box<dyn std::error::Error>> {
    opentelemetry::global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    // Create the exporter asynchronously
    let exporter = futures::executor::block_on(ClickhouseExporter::new(config))?;

    // Build a tracer provider with a batch processor using the async exporter
    let provider = sdktrace::TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio) // Make sure runtime::Tokio is appropriate
        .with_config(sdktrace::config().with_resource(Resource::new(vec![
            KeyValue::new("service.name", "my-service-example"),
            KeyValue::new("service.version", "1.0.1"),
            KeyValue::new("deployment.environment", "development"),
        ])))
        .build();

    // Use the TracerProvider trait method
    let tracer = provider.tracer("my-app-example", Some("v0.1.0"));
    global::set_tracer_provider(provider);
    Ok(tracer)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load DSN from environment or use default
    let clickhouse_dsn = env::var("CLICKHOUSE_DSN")
        .unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".to_string());

    // Configure the exporter
    let exporter_config = ClickhouseExporterConfig::new(clickhouse_dsn)?
        .with_schema_creation(true) // Ensure tables are created if they don't exist
        .with_spans_table("otel_spans_example"); // Use specific table names for the example

    // Initialize the tracer
    let tracer = init_tracer(exporter_config)?;

    // -- Example Trace Start --

    // Root span: simulates an incoming request or main operation
    tracer.in_span("root-operation", async |cx| { // Made closure async
        let span = cx.span();
        span.set_attribute(KeyValue::new("app.user.id", "user-12345"));
        span.add_event(
            "Received request",
            vec![KeyValue::new("http.method", "GET"), KeyValue::new("http.url", "/api/resource")]
        );

        // Simulate some work
        tokio::time::sleep(Duration::from_millis(15)).await; // Use await inside async closure

        // Child span: simulates a database query within the request
        tracer.in_span("child-db-query", async |cx_child| { // Made closure async
            let child_span = cx_child.span();

            // Set SpanKind using attribute
            child_span.set_attribute(KeyValue::new(
                attribute::SPAN_KIND, // Use constant from attribute module
                span_kind_to_string(&SpanKind::Client)
            ));
            // Set other DB related attributes using semantic conventions
            child_span.set_attribute(KeyValue::new(attribute::DB_SYSTEM, "clickhouse")); // Use attribute::DB_SYSTEM
            child_span.set_attribute(KeyValue::new(attribute::DB_NAME, "example_db"));
            child_span.set_attribute(KeyValue::new(
                attribute::DB_STATEMENT,
                "SELECT * FROM users WHERE id = ?",
            ));

            // Simulate DB query time
            tokio::time::sleep(Duration::from_millis(35)).await; // Use await inside async closure

            // Simulate an error scenario
            let simulated_error = true;
            if simulated_error {
                let err_msg = "Simulated database connection error".to_string();
                child_span.set_status(Status::Error { description: err_msg.clone().into() });
                // Borrow the error for record_error
                child_span.record_error(&std::io::Error::new(std::io::ErrorKind::ConnectionRefused, err_msg));
                child_span.add_event("Database query failed", vec![]);
            } else {
                child_span.set_status(Status::Ok);
                child_span.add_event("Database query successful", vec![KeyValue::new("db.rows_returned", 1)]);
            }
        }); // End child span

        span.add_event("Finished processing request", vec![]);

    }); // End root span

    // -- Example Trace End --

    // Shut down the provider to ensure all spans are flushed
    global::shutdown_tracer_provider();

    println!("Trace finished. Check your ClickHouse `otel_spans_example` table.");

    Ok(())
}
