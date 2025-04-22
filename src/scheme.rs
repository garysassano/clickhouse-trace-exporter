use crate::config::ClickhouseExporterConfig;
use crate::error::ClickhouseExporterError;
use clickhouse::Client; // Use correct crate name

// --- SQL Schema Definitions ---
// NOTE: Carefully review and adjust types (DateTime64 precision, String vs FixedString/UUID, Map types)
// based on your ClickHouse version, `clickhouse` crate features, and query needs.

fn get_spans_schema(table_name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {table_name} (
            Timestamp DateTime64(9, 'UTC'), -- Recommended: Explicit precision and UTC timezone
            TraceId String,               -- Consider FixedString(32) or UUID if IDs are hex/UUIDs
            SpanId String,                -- Consider FixedString(16)
            ParentSpanId String,          -- Consider FixedString(16)
            TraceState String,
            SpanName String,
            SpanKind LowCardinality(String), -- Good candidate for LowCardinality
            ServiceName LowCardinality(String), -- Good candidate for LowCardinality
            ResourceAttributes Map(LowCardinality(String), String), -- CH Map type for key-value
            ScopeName String,
            ScopeVersion String,
            SpanAttributes Map(LowCardinality(String), String), -- CH Map type for key-value
            Duration Int64,               -- Nanoseconds (signed to match OTel spec possibility)
            StatusCode LowCardinality(String), -- Ok, Error, Unset
            StatusMessage String,
            Events Nested (                 -- Nested structure for events
                Timestamp DateTime64(9, 'UTC'),
                Name String,
                Attributes Map(LowCardinality(String), String)
            ),
            Links Nested (                 -- Nested structure for links
                TraceId String,
                SpanId String,
                TraceState String,
                Attributes Map(LowCardinality(String), String)
            ),
            INDEX idx_trace_id TraceId TYPE bloom_filter GRANULARITY 1,
            INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter GRANULARITY 1,
            INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter GRANULARITY 1,
            INDEX idx_svc_name ServiceName TYPE bloom_filter GRANULARITY 1,
            INDEX idx_duration Duration TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(Timestamp) -- Common partitioning strategy
        ORDER BY (ServiceName, SpanName, Timestamp, TraceId) -- Choose ORDER BY based on common queries
        SETTINGS index_granularity = 8192;
        "#
    )
}

// Optional: Schema for flattened attributes if you prefer that over Maps
// fn get_attributes_schema(table_name: &str) -> String { ... }

fn get_errors_schema(table_name: &str) -> String {
    // Table specifically for extracted error events for easier querying
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS {table_name} (
            Timestamp DateTime64(9, 'UTC'),
            TraceId String,
            SpanId String,
            ServiceName LowCardinality(String),
            SpanName String,
            ErrorKind String, -- e.g., exception.type
            ErrorMessage String, -- e.g., exception.message
            ErrorStacktrace String, -- e.g., exception.stacktrace
            ErrorAttributes Map(LowCardinality(String), String) -- Attributes associated with the error event
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(Timestamp)
        ORDER BY (ServiceName, SpanName, Timestamp)
        SETTINGS index_granularity = 8192;
        "#
    )
}

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
    let errors_sql = get_errors_schema(&config.errors_table_name);
    // let attributes_sql = get_attributes_schema(&config.attributes_table_name); // If using flattened attrs

    // Execute schema creation queries sequentially
    client.query(&spans_sql).execute().await.map_err(|e| {
        tracing::error!(
            "Failed to create/check spans table '{}': {}",
            config.spans_table_name,
            e
        );
        ClickhouseExporterError::SchemaCreationError(e)
    })?;
    tracing::info!("Checked/Created table: {}", config.spans_table_name);

    client.query(&errors_sql).execute().await.map_err(|e| {
        tracing::error!(
            "Failed to create/check errors table '{}': {}",
            config.errors_table_name,
            e
        );
        ClickhouseExporterError::SchemaCreationError(e)
    })?;
    tracing::info!("Checked/Created table: {}", config.errors_table_name);

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
