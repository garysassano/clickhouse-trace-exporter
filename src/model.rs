use chrono::{DateTime, LocalResult, TimeZone, Utc};
use clickhouse::Row;
use opentelemetry::{
    trace::{Link, SpanEvent, SpanKind, Status},
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

pub(crate) fn duration_to_nanos(duration: Duration) -> i64 {
    duration.as_nanos() as i64
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
        Value::Array(arr) => format!("{:?}", arr),
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

// --- New Helper Functions for Flattening ---

// Converts OTel events into parallel arrays for ClickHouse Nested columns
pub(crate) fn convert_events(
    events: &Cow<'_, [SpanEvent]>,
) -> (
    Vec<DateTime<Utc>>,
    Vec<String>,
    Vec<HashMap<String, String>>,
) {
    let mut times = Vec::with_capacity(events.len());
    let mut names = Vec::with_capacity(events.len());
    let mut attrs = Vec::with_capacity(events.len());

    for event in events.iter() {
        times.push(system_time_to_utc(event.timestamp));
        names.push(event.name.to_string());
        attrs.push(attributes_to_map(&event.attributes));
    }
    (times, names, attrs)
}

// Converts OTel links into parallel arrays for ClickHouse Nested columns
pub(crate) fn convert_links(
    links: &Cow<'_, [Link]>,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<HashMap<String, String>>,
) {
    let mut trace_ids = Vec::with_capacity(links.len());
    let mut span_ids = Vec::with_capacity(links.len());
    let mut trace_states = Vec::with_capacity(links.len());
    let mut attrs = Vec::with_capacity(links.len());

    for link in links.iter() {
        trace_ids.push(link.span_context.trace_id().to_string());
        span_ids.push(link.span_context.span_id().to_string());
        trace_states.push(link.span_context.trace_state().header().to_string());
        attrs.push(attributes_to_map(&link.attributes));
    }
    (trace_ids, span_ids, trace_states, attrs)
}

// --- Removed Structs ---
// Removed SpanRow, EventRow, LinkRow, ErrorRow
// Removed convert_otel_span_to_rows
