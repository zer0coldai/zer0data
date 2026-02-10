CREATE DATABASE IF NOT EXISTS zer0data;

USE zer0data;

CREATE TABLE IF NOT EXISTS klines (
    symbol String,
    open_time UInt64,
    close_time UInt64,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    quote_volume Float64,
    trades_count UInt32,
    taker_buy_volume Float64,
    taker_buy_quote_volume Float64
) ENGINE = MergeTree()
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;
