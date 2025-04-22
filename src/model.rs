use chrono::{DateTime, LocalResult, TimeZone, Utc};
use opentelemetry::{
    trace::{Event as SpanEvent, Link, SpanKind, Status},
    Key, KeyValue, Value,
};
use opentelemetry_sdk::{export::trace::SpanLinks, Resource};
use opentelemetry_semantic_conventions::{resource, trace};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// Import the structs defined in schema.rs for Nested types
use crate::schema::{EventRow, LinkRow};

// --- Utility Functions ---

pub(crate) fn system_time_to_utc(st: SystemTime) -> DateTime<Utc> {
    let epoch_duration = st.duration_since(UNIX_EPOCH).unwrap_or_default();
    match Utc.timestamp_opt(
        epoch_duration.as_secs() as i64,
        epoch_duration.subsec_nanos(),
    ) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => Utc::now(), // Fallback or handle error
    }
}

// Changed return type to u64 to match schema
pub(crate) fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos() as u64
}

pub(crate) fn span_kind_to_string(kind: &SpanKind) -> String {
    match kind {
        SpanKind::Client => "Client",
        SpanKind::Server => "Server",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
        SpanKind::Internal => "Internal",
    }
    .to_string()
}

pub(crate) fn status_code_to_string(status: &Status) -> String {
    match status {
        Status::Ok => "Ok",
        Status::Error { .. } => "Error",
        Status::Unset => "Unset",
    }
    .to_string()
}

pub(crate) fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::F64(f) => f.to_string(),
        Value::I64(i) => i.to_string(),
        Value::Array(arr) => format!("{:?}", arr), // Basic array formatting
    }
}

pub(crate) fn attributes_to_map<'a>(
    attrs: impl IntoIterator<Item = &'a KeyValue>,
) -> HashMap<String, String> {
    attrs
        .into_iter()
        .map(|kv| (kv.key.to_string(), value_to_string(&kv.value)))
        .collect()
}

pub(crate) fn get_service_name(resource: &Resource) -> String {
    resource
        .get(resource::SERVICE_NAME.into())
        .map_or_else(|| "unknown_service".to_string(), |v| value_to_string(&v))
}

// --- Updated Helper Functions for Nested Types ---

// Converts OTel events into a Vec of EventRow for ClickHouse Nested columns
pub(crate) fn convert_events(events: &Cow<'_, [SpanEvent]>) -> Vec<EventRow> {
    events
        .iter()
        .map(|event| EventRow {
            timestamp: system_time_to_utc(event.timestamp),
            name: event.name.to_string(),
            attributes: attributes_to_map(&event.attributes),
        })
        .collect()
}

// Converts OTel links into a Vec of LinkRow for ClickHouse Nested columns
pub(crate) fn convert_links(links: &SpanLinks) -> Vec<LinkRow> {
    links
        .iter()
        .map(|link| LinkRow {
            trace_id: link.span_context.trace_id().to_string(),
            span_id: link.span_context.span_id().to_string(),
            trace_state: link.span_context.trace_state().header().to_string(),
            attributes: attributes_to_map(&link.attributes),
        })
        .collect()
}

// --- Removed Structs ---
// Removed SpanRow, EventRow, LinkRow, ErrorRow
// Removed convert_otel_span_to_rows
