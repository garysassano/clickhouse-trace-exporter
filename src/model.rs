use chrono::{DateTime, TimeZone, Utc};
use opentelemetry::trace::{SpanKind, StatusCode};
use opentelemetry::{Key, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::export::trace::SpanData;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// --- Utility Functions ---

/// Converts OTel SystemTime to chrono DateTime<Utc> needed by clickhouse crate.
/// Preserves nanosecond precision if the DateTime64(9) type is used in CH.
fn system_time_to_chrono_utc(st: SystemTime) -> DateTime<Utc> {
    let epoch_duration = st.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    Utc.timestamp_opt(
        epoch_duration.as_secs() as i64,
        epoch_duration.subsec_nanos(),
    )
    .unwrap_or_default() // Should not fail for valid SystemTime
}

/// Maps OTel SpanKind enum to a string representation.
fn span_kind_to_string(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Client => "CLIENT",
        SpanKind::Server => "SERVER",
        SpanKind::Producer => "PRODUCER",
        SpanKind::Consumer => "CONSUMER",
        SpanKind::Internal => "INTERNAL",
    }
}

/// Maps OTel StatusCode enum to a string representation.
fn status_code_to_string(code: StatusCode) -> &'static str {
    match code {
        StatusCode::Ok => "OK",
        StatusCode::Error => "ERROR",
        StatusCode::Unset => "UNSET",
    }
}

/// Converts OTel KeyValue attributes/links/events attributes to a HashMap<String, String>.
/// Note: This simplifies all values to strings. Handle numeric/boolean types differently if needed.
fn attributes_to_map(attrs: &[(Key, Value)]) -> HashMap<String, String> {
    attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// --- Row Structs Matching Schema ---
// These structs derive `clickhouse::Row` and `serde::Serialize` for insertion.
// Field names match the `CREATE TABLE` statements. Use `#[serde(rename = "...")]` if needed.

#[derive(clickhouse::Row, Serialize, Debug, Clone)]
pub(crate) struct SpanRow {
    #[serde(rename = "Timestamp")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "ParentSpanId")]
    parent_span_id: String,
    #[serde(rename = "TraceState")]
    trace_state: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "SpanKind")]
    span_kind: &'static str,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "SpanAttributes")]
    span_attributes: HashMap<String, String>,
    #[serde(rename = "Duration")]
    duration: i64, // Nanoseconds
    #[serde(rename = "StatusCode")]
    status_code: &'static str,
    #[serde(rename = "StatusMessage")]
    status_message: String,
    #[serde(rename = "Events")]
    events: Vec<EventRow>, // Mapped to Nested structure
    #[serde(rename = "Links")]
    links: Vec<LinkRow>, // Mapped to Nested structure
}

#[derive(clickhouse::Row, Serialize, Debug, Clone)]
pub(crate) struct EventRow {
    #[serde(rename = "Timestamp")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Attributes")]
    attributes: HashMap<String, String>,
}

#[derive(clickhouse::Row, Serialize, Debug, Clone)]
pub(crate) struct LinkRow {
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "TraceState")]
    trace_state: String,
    #[serde(rename = "Attributes")]
    attributes: HashMap<String, String>,
}

#[derive(clickhouse::Row, Serialize, Debug, Clone)]
pub(crate) struct ErrorRow {
    #[serde(rename = "Timestamp")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "ErrorKind")]
    error_kind: String,
    #[serde(rename = "ErrorMessage")]
    error_message: String,
    #[serde(rename = "ErrorStacktrace")]
    error_stacktrace: String,
    #[serde(rename = "ErrorAttributes")]
    error_attributes: HashMap<String, String>,
}

// --- Conversion Logic ---

/// Converts a single OTel `SpanData` into rows for ClickHouse tables.
pub(crate) fn convert_otel_span_to_rows(
    span_data: &SpanData,
    resource: &Resource, // Pass resource explicitly for clarity
) -> (SpanRow, Vec<ErrorRow>) {
    // Extract common info
    let trace_id = span_data.span_context.trace_id().to_string();
    let span_id = span_data.span_context.span_id().to_string();
    let start_time = system_time_to_chrono_utc(span_data.start_time);
    let service_name = resource
        .get(opentelemetry_semantic_conventions::resource::SERVICE_NAME.into())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown_service".to_string());

    // Prepare nested structures
    let events: Vec<EventRow> = span_data
        .events
        .iter()
        .map(|ev| EventRow {
            timestamp: system_time_to_chrono_utc(ev.timestamp),
            name: ev.name.to_string(),
            attributes: attributes_to_map(&ev.attributes),
        })
        .collect();

    let links: Vec<LinkRow> = span_data
        .links
        .iter()
        .map(|link| LinkRow {
            trace_id: link.span_context().trace_id().to_string(),
            span_id: link.span_context().span_id().to_string(),
            trace_state: link.span_context().trace_state().header(),
            attributes: attributes_to_map(&link.attributes()),
        })
        .collect();

    // Calculate duration
    let duration_nanos = span_data
        .end_time
        .duration_since(span_data.start_time)
        .map(|d| d.as_nanos())
        .unwrap_or(0) as i64; // Use i64 for ClickHouse Int64

    // Create the main SpanRow
    let span_row = SpanRow {
        timestamp: start_time,
        trace_id: trace_id.clone(), // Clone needed for error rows potentially
        span_id: span_id.clone(),   // Clone needed for error rows potentially
        parent_span_id: span_data
            .parent_span_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        trace_state: span_data.span_context.trace_state().header(),
        span_name: span_data.name.to_string(),
        span_kind: span_kind_to_string(span_data.kind),
        service_name: service_name.clone(), // Clone needed for error rows
        resource_attributes: resource
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        scope_name: span_data.instrumentation_library.name.to_string(),
        scope_version: span_data
            .instrumentation_library
            .version
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default(),
        span_attributes: attributes_to_map(&span_data.attributes),
        duration: duration_nanos,
        status_code: status_code_to_string(span_data.status.code()),
        status_message: span_data.status.description().to_string(),
        events,
        links,
    };

    // --- Extract Error Events ---
    // Look for events conventionaly representing exceptions/errors.
    let error_rows: Vec<ErrorRow> = span_data
        .events
        .iter()
        // Check for OTel semantic convention "exception" event name
        .filter(|ev| ev.name == opentelemetry_semantic_conventions::trace::EXCEPTION_EVENT_NAME)
        .map(|ev| {
            let mut error_kind = String::new();
            let mut error_message = String::new();
            let mut error_stacktrace = String::new();
            let mut error_attributes = HashMap::new();

            // Extract standard exception fields
            for (key, value) in &ev.attributes {
                let k = key.as_str();
                let v = value.to_string(); // Simplify to string for example
                match k {
                    opentelemetry_semantic_conventions::trace::EXCEPTION_TYPE => error_kind = v,
                    opentelemetry_semantic_conventions::trace::EXCEPTION_MESSAGE => {
                        error_message = v
                    }
                    opentelemetry_semantic_conventions::trace::EXCEPTION_STACKTRACE => {
                        error_stacktrace = v
                    }
                    _ => {
                        // Store other attributes associated with the exception event
                        error_attributes.insert(k.to_string(), v);
                    }
                }
            }

            ErrorRow {
                timestamp: system_time_to_chrono_utc(ev.timestamp),
                trace_id: trace_id.clone(),            // Use cloned ID
                span_id: span_id.clone(),              // Use cloned ID
                service_name: service_name.clone(),    // Use cloned service name
                span_name: span_data.name.to_string(), // Add span name context
                error_kind,
                error_message,
                error_stacktrace,
                error_attributes,
            }
        })
        .collect();

    (span_row, error_rows)
}

// --- Optional: Conversion for Flattened Attributes ---
// pub(crate) fn convert_span_to_attribute_rows(...) -> Vec<AttributeRow> { ... }
