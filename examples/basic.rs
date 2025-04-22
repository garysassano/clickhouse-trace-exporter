//! Basic exporter example
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{KeyValue, global};
use opentelemetry_clickhouse_exporter::{
    ClickhouseExporter, ClickhouseExporterConfig, model::span_kind_to_string,
};
use opentelemetry_sdk::{resource::Resource, trace as sdktrace};
use std::env;
use std::time::Duration;

fn init_tracer(
    config: ClickhouseExporterConfig,
) -> Result<sdktrace::Tracer, Box<dyn std::error::Error>> {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    // Create the exporter asynchronously
    let exporter = futures::executor::block_on(ClickhouseExporter::new(config))?;

    // Create resource with service information via ResourceBuilder
    let resource = Resource::builder()
        .with_service_name("my-service-example")
        .with_attribute(KeyValue::new("service.version", "1.0.1"))
        .with_attribute(KeyValue::new("deployment.environment", "development"))
        .build();

    // Build a tracer provider with batch span processor (uses tokio runtime)
    let provider = sdktrace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    // Create a tracer
    let tracer = provider.tracer("my-app-example");

    // Set as global provider
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
    tracer.in_span("root-operation", |cx| {
        let span = cx.span();
        span.set_attribute(KeyValue::new("app.user.id", "user-12345"));
        span.add_event(
            "Received request",
            vec![
                KeyValue::new("http.method", "GET"),
                KeyValue::new("http.url", "/api/resource"),
            ],
        );

        // Simulate some work
        std::thread::sleep(Duration::from_millis(15));

        // Child span: simulates a database query within the request
        tracer.in_span("child-db-query", |cx_child| {
            let child_span = cx_child.span();

            // Set SpanKind using attribute
            child_span.set_attribute(KeyValue::new(
                "span.kind",
                span_kind_to_string(&SpanKind::Client),
            ));

            // Set other DB related attributes
            child_span.set_attribute(KeyValue::new("db.system", "clickhouse"));
            child_span.set_attribute(KeyValue::new("db.name", "example_db"));
            child_span.set_attribute(KeyValue::new(
                "db.statement",
                "SELECT * FROM users WHERE id = ?",
            ));

            // Simulate DB query time
            std::thread::sleep(Duration::from_millis(35));

            // Simulate an error scenario
            let simulated_error = true;
            if simulated_error {
                let err_msg = "Simulated database connection error".to_string();
                child_span.set_status(Status::Error {
                    description: err_msg.clone().into(),
                });
                // Borrow the error for record_error
                child_span.record_error(&std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    err_msg,
                ));
                child_span.add_event("Database query failed", vec![]);
            } else {
                child_span.set_status(Status::Ok);
                child_span.add_event(
                    "Database query successful",
                    vec![KeyValue::new("db.rows_returned", 1)],
                );
            }
        });

        span.add_event("Finished processing request", vec![]);
    });

    // -- Example Trace End --

    println!("Trace finished. Check your ClickHouse `otel_spans_example` table.");
    // Shutdown the tracer provider to flush any remaining spans
    global::shutdown_tracer_provider();
    Ok(())
}
