CREATE DATABASE IF NOT EXISTS zer0data;

CREATE TABLE IF NOT EXISTS zer0data.raw_exchange_info
(
    pulled_at DateTime64(3, 'UTC'),
    market LowCardinality(String),
    endpoint LowCardinality(String),
    source_url String,
    status_code UInt16,
    latency_ms UInt32,
    payload String CODEC(ZSTD(6)),
    payload_hash FixedString(64),
    ingest_version UInt64 DEFAULT toUnixTimestamp64Milli(now64(3)),
    err_msg Nullable(String)
)
ENGINE = ReplacingMergeTree(ingest_version)
PARTITION BY toYYYYMM(pulled_at)
ORDER BY (market, endpoint, pulled_at, payload_hash)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS zer0data.factors
(
    symbol LowCardinality(String),
    datetime DateTime64(3, 'UTC'),
    factor_name LowCardinality(String),
    factor_value Float64,
    source LowCardinality(String) DEFAULT 'coinmetrics',
    update_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(update_time)
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime, factor_name)
SETTINGS index_granularity = 8192;
