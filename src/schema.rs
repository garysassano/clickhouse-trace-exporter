use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row}; // Use Client, not Pool. Added Row for Nested types

// --- SQL Schema Definitions ---
// NOTE: Carefully review and adjust types (DateTime64 precision, String vs FixedString/UUID, Map types)
// based on your ClickHouse version, `clickhouse` crate features, and query needs.

// Helper structs for Nested types
#[derive(Row, Debug, Clone, serde::Serialize)]
pub(crate) struct EventRow {
    #[serde(rename = "Events.Timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "Events.Name")]
    pub name: String,
    #[serde(rename = "Events.Attributes")]
    pub attributes: Vec<(String, String)>,
}

#[derive(Row, Debug, Clone, serde::Serialize)]
pub(crate) struct LinkRow {
    #[serde(rename = "Links.TraceId")]
    pub trace_id: String,
    #[serde(rename = "Links.SpanId")]
    pub span_id: String,
    #[serde(rename = "Links.TraceState")]
    pub trace_state: String,
    #[serde(rename = "Links.Attributes")]
    pub attributes: Vec<(String, String)>,
}

fn get_spans_schema(table_name: &str, config: &ClickhouseExporterConfig) -> String {
    // Build ON CLUSTER, ENGINE, and TTL clauses
    let cluster = config.cluster_string();
    let engine = config.table_engine_string();
    let ttl = config.ttl_expr("toDateTime(Timestamp)");
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {table_name} {cluster} (
            Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
            TraceId String CODEC(ZSTD(1)),
            SpanId String CODEC(ZSTD(1)),
            ParentSpanId String CODEC(ZSTD(1)),
            TraceState String CODEC(ZSTD(1)),
            SpanName LowCardinality(String) CODEC(ZSTD(1)),
            SpanKind LowCardinality(String) CODEC(ZSTD(1)),
            ServiceName LowCardinality(String) CODEC(ZSTD(1)),
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ScopeName String CODEC(ZSTD(1)),
            ScopeVersion String CODEC(ZSTD(1)),
            SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            Duration UInt64 CODEC(ZSTD(1)),
            StatusCode LowCardinality(String) CODEC(ZSTD(1)),
            StatusMessage String CODEC(ZSTD(1)),
            Events Nested (
                Timestamp DateTime64(9),
                Name LowCardinality(String),
                Attributes Map(LowCardinality(String), String)
            ) CODEC(ZSTD(1)),
            Links Nested (
                TraceId String,
                SpanId String,
                TraceState String,
                Attributes Map(LowCardinality(String), String)
            ) CODEC(ZSTD(1)),

            INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
            INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
            INDEX idx_svc_name ServiceName TYPE bloom_filter GRANULARITY 1,
            INDEX idx_duration Duration TYPE minmax GRANULARITY 1
        ) ENGINE = {engine}
        PARTITION BY toDate(Timestamp)
        ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
        {ttl}
        SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
        "#,
        table_name = table_name,
        cluster = cluster,
        engine = engine,
        ttl = ttl
    )
}

// Optional: Schema for flattened attributes if you prefer that over Maps
// fn get_attributes_schema(table_name: &str) -> String { ... }

/// Executes `CREATE TABLE IF NOT EXISTS` statements if config.create_schema is true.
pub(crate) async fn ensure_schema(
    client: &Client,
    config: &ClickhouseExporterConfig,
) -> Result<(), ClickhouseExporterError> {
    if !config.create_schema {
        tracing::debug!("Schema creation skipped as per configuration.");
        return Ok(());
    }
    tracing::info!("Ensuring ClickHouse schema exists...");

    let spans_sql = get_spans_schema(&config.spans_table_name, config);
    // let attributes_sql = get_attributes_schema(&config.attributes_table_name); // If using flattened attrs

    // Execute schema creation queries sequentially using the passed client
    client.query(&spans_sql).execute().await.map_err(|e| {
        tracing::error!(
            "Failed to create/check spans table '{}': {}",
            config.spans_table_name,
            e
        );
        // Assuming ClickhouseError can represent schema errors too - Use ClickhouseClientError variant
        ClickhouseExporterError::ClickhouseClientError(e)
    })?;
    tracing::info!("Checked/Created table: {}", config.spans_table_name);

    // if config.use_flattened_attributes { // Example condition
    //     client
    //         .query(&attributes_sql)
    //         .execute()
    //         .await
    //         .map_err(ClickhouseExporterError::SchemaCreationError)?;
    //     tracing::info!("Checked/created table: {}", config.attributes_table_name);
    // }

    tracing::info!("ClickHouse schema check complete.");
    Ok(())
}
