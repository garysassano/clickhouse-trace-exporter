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

fn get_spans_schema(table_name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {table_name} (
            Timestamp DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)), -- Explicit UTC, added Codec
            TraceId String CODEC(ZSTD(1)),               -- Consider FixedString(32) or UUID. Added Codec
            SpanId String CODEC(ZSTD(1)),                -- Consider FixedString(16). Added Codec
            ParentSpanId String CODEC(ZSTD(1)),          -- Consider FixedString(16). Added Codec
            TraceState String CODEC(ZSTD(1)),            -- Added Codec
            SpanName LowCardinality(String) CODEC(ZSTD(1)), -- Use LowCardinality. Added Codec
            SpanKind LowCardinality(String) CODEC(ZSTD(1)), -- Use LowCardinality. Added Codec
            ServiceName LowCardinality(String) CODEC(ZSTD(1)), -- Use LowCardinality. Added Codec
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- Added Codec
            ScopeName String CODEC(ZSTD(1)),             -- Added Codec
            ScopeVersion String CODEC(ZSTD(1)),          -- Added Codec
            SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)), -- Added Codec
            Duration UInt64 CODEC(ZSTD(1)),               -- Changed to UInt64 (match Go). Added Codec
            StatusCode LowCardinality(String) CODEC(ZSTD(1)), -- Use LowCardinality. Added Codec
            StatusMessage String CODEC(ZSTD(1)),         -- Added Codec
            Events Nested (                         -- Use Nested type
                Timestamp DateTime64(9, 'UTC'),
                Name LowCardinality(String),
                Attributes Map(LowCardinality(String), String)
            ) CODEC(ZSTD(1)),                        -- Added Codec for Nested
            Links Nested (                          -- Use Nested type
                TraceId String,
                SpanId String,
                TraceState String,
                Attributes Map(LowCardinality(String), String)
            ) CODEC(ZSTD(1)),                        -- Added Codec for Nested

            INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1, -- Adjusted bloom_filter parameter
            INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1, -- Added bloom_filter parameter
            INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1, -- Added index on map values
            INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1, -- Added bloom_filter parameter
            INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1, -- Added index on map values
            INDEX idx_svc_name ServiceName TYPE bloom_filter GRANULARITY 1, -- Kept this index
            INDEX idx_duration Duration TYPE minmax GRANULARITY 1 -- Kept this index
        ) ENGINE = MergeTree()
        PARTITION BY toDate(Timestamp) -- Match Go partitioning
        ORDER BY (ServiceName, SpanName, toUnixTimestamp64Nano(Timestamp)) -- Match Go ordering (use nano for accuracy)
        SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1; -- Match Go settings
        "#
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

    let spans_sql = get_spans_schema(&config.spans_table_name);
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
