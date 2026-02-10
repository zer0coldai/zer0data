CREATE DATABASE IF NOT EXISTS zer0data;

USE zer0data;

CREATE TABLE IF NOT EXISTS klines_1m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(8),
    high_price Decimal64(8),
    low_price Decimal64(8),
    close_price Decimal64(8),
    volume Decimal64(8),
    quote_volume Decimal64(8),
    trades_count UInt32,
    taker_buy_volume Decimal64(8),
    taker_buy_quote_volume Decimal64(8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;
