use chrono::{DateTime, LocalResult, TimeZone, Utc};
use clickhouse::Row;
use opentelemetry::{
    trace::{Link, SpanKind, Status},
    Key, KeyValue, Value,
};
use opentelemetry_sdk::{export::trace::SpanData, Resource};
use opentelemetry_semantic_conventions::{resource, trace};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// --- Utility Functions ---

fn system_time_to_utc(st: SystemTime) -> DateTime<Utc> {
    let epoch_duration = st.duration_since(UNIX_EPOCH).unwrap_or_default();
    // Use .single() to resolve LocalResult from timestamp_opt
    match Utc.timestamp_opt(
        epoch_duration.as_secs() as i64,
        epoch_duration.subsec_nanos(),
    ) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => Utc::now(), // Fallback or handle error
    }
}

fn duration_to_nanos(duration: Duration) -> i64 {
    duration.as_nanos() as i64 // Consider potential overflow if duration is extremely large
}

fn span_kind_to_string(kind: &SpanKind) -> String {
    match kind {
        SpanKind::Client => "Client",
        SpanKind::Server => "Server",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
        SpanKind::Internal => "Internal",
    }
    .to_string()
}

fn status_code_to_string(status: &Status) -> String {
    match status {
        Status::Ok => "Ok",
        Status::Error { .. } => "Error",
        Status::Unset => "Unset",
    }
    .to_string()
}

// Convert OTel Value to String. Adjust based on how you want to handle complex types (arrays, etc.)
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::F64(f) => f.to_string(),
        Value::I64(i) => i.to_string(),
        Value::Array(arr) => format!("{:?}", arr), // Basic array representation
                                                   // Add other types as needed (Bytes, etc.)
    }
}

// Convert KeyValue iterator to HashMap<String, String> for ClickHouse Map type
// Made pub(crate) to be accessible from exporter.rs
pub(crate) fn attributes_to_map<'a>(
    attrs: impl IntoIterator<Item = &'a KeyValue>,
) -> HashMap<String, String> {
    attrs
        .into_iter()
        .map(|kv| (kv.key.to_string(), value_to_string(&kv.value)))
        .collect()
}

// --- ClickHouse Row Structures --- // (Keep #[derive(Row, ...)] as clickhouse-derive handles it)

#[derive(Row, Serialize, Debug, Clone)]
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
    span_kind: String,
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
    duration: i64,
    #[serde(rename = "StatusCode")]
    status_code: String,
    #[serde(rename = "StatusMessage")]
    status_message: String,
    #[serde(rename = "Events")]
    events: Vec<EventRow>,
    #[serde(rename = "Links")]
    links: Vec<LinkRow>,
}

#[derive(Row, Serialize, Debug, Clone)]
pub(crate) struct EventRow {
    #[serde(rename = "Timestamp")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Attributes")]
    attributes: HashMap<String, String>,
}

#[derive(Row, Serialize, Debug, Clone)]
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

#[derive(Row, Serialize, Debug, Clone)]
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

// Helper to extract service name from resource attributes
fn get_service_name(resource: &Resource) -> String {
    resource
        .get(resource::SERVICE_NAME.into())
        .map_or_else(|| "unknown_service".to_string(), |v| value_to_string(&v))
}

// Convert OTel SpanData to ClickHouse rows (SpanRow and potentially ErrorRows)
pub(crate) fn convert_otel_span_to_rows(
    span_data: &SpanData,
    resource: &Resource,
) -> (SpanRow, Vec<ErrorRow>) {
    let service_name = get_service_name(resource);
    // Pass iterator directly to attributes_to_map
    let resource_attributes = attributes_to_map(
        resource
            .iter()
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect::<Vec<_>>()
            .iter(),
    ); // Need KeyValue, not (&Key, &Value)
    let span_attributes = attributes_to_map(&span_data.attributes);

    let events: Vec<EventRow> = span_data
        .events
        .iter()
        .map(|ev| EventRow {
            timestamp: system_time_to_utc(ev.timestamp),
            name: ev.name.to_string(),
            attributes: attributes_to_map(&ev.attributes), // Pass slice
        })
        .collect();

    let links: Vec<LinkRow> = span_data
        .links
        .iter()
        .map(|link: &Link| LinkRow {
            // Use field access, not methods
            trace_id: link.span_context.trace_id().to_string(),
            span_id: link.span_context.span_id().to_string(),
            trace_state: link.span_context.trace_state().header().to_string(),
            attributes: attributes_to_map(&link.attributes), // Pass slice
        })
        .collect();

    let status_message = match &span_data.status {
        Status::Error { description } => description.to_string(),
        _ => "".to_string(),
    };

    let span_row = SpanRow {
        timestamp: system_time_to_utc(span_data.start_time),
        trace_id: span_data.span_context.trace_id().to_string(),
        span_id: span_data.span_context.span_id().to_string(),
        parent_span_id: span_data.parent_span_id.to_string(), // Use .to_string() directly
        trace_state: span_data.span_context.trace_state().header().to_string(),
        span_name: span_data.name.to_string(),
        // Use field span_kind instead of kind, pass by reference
        span_kind: span_kind_to_string(&span_data.span_kind),
        service_name: service_name.clone(), // Clone service_name for SpanRow
        resource_attributes,
        // Use field instrumentation_lib instead of instrumentation_library
        scope_name: span_data.instrumentation_lib.name.to_string(),
        scope_version: span_data
            .instrumentation_lib
            .version
            .map_or("".to_string(), |v| v.to_string()),
        span_attributes,
        duration: span_data
            .end_time
            .duration_since(span_data.start_time)
            .map(duration_to_nanos)
            .unwrap_or(0),
        status_code: status_code_to_string(&span_data.status),
        status_message,
        events,
        links,
    };

    // Extract error events into ErrorRow (optional)
    let error_rows: Vec<ErrorRow> = span_data
        .events
        .iter()
        // Use constant directly from module
        .filter(|ev| ev.name.as_ref() == trace::EXCEPTION_EVENT_NAME.to_string())
        .map(|ev| {
            let mut error_kind = "".to_string();
            let mut error_message = "".to_string();
            let mut error_stacktrace = "".to_string();
            let mut error_attributes = HashMap::new();

            for kv in &ev.attributes {
                let key_str = kv.key.as_str();
                let value_str = value_to_string(&kv.value);

                // Use constants directly from module
                if key_str == trace::EXCEPTION_TYPE.to_string() {
                    error_kind = value_str.clone();
                } else if key_str == trace::EXCEPTION_MESSAGE.to_string() {
                    error_message = value_str.clone();
                } else if key_str == trace::EXCEPTION_STACKTRACE.to_string() {
                    error_stacktrace = value_str.clone();
                }
                error_attributes.insert(key_str.to_string(), value_str);
            }

            ErrorRow {
                timestamp: system_time_to_utc(ev.timestamp),
                trace_id: span_data.span_context.trace_id().to_string(),
                span_id: span_data.span_context.span_id().to_string(),
                service_name: get_service_name(resource),
                span_name: span_data.name.to_string(),
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
