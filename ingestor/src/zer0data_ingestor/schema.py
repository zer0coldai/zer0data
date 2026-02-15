"""Schema definitions for kline data.

Centralizes column names, data types, and Binance CSV mappings.
All modules reference these constants instead of hard-coding column names.
"""

# Columns in our internal DataFrame / ClickHouse tables.
KLINE_COLUMNS = [
    "symbol",
    "open_time",
    "close_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trades_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "interval",
]

# pandas dtypes for each column.
KLINE_DTYPES = {
    "symbol": "object",
    "open_time": "int64",
    "close_time": "int64",
    "open_price": "float64",
    "high_price": "float64",
    "low_price": "float64",
    "close_price": "float64",
    "volume": "float64",
    "quote_volume": "float64",
    "trades_count": "int64",
    "taker_buy_volume": "float64",
    "taker_buy_quote_volume": "float64",
    "interval": "object",
}

# ClickHouse column types (used for CREATE TABLE).
CLICKHOUSE_COLUMN_TYPES = {
    "symbol": "String",
    "open_time": "Int64",
    "close_time": "Int64",
    "open_price": "Float64",
    "high_price": "Float64",
    "low_price": "Float64",
    "close_price": "Float64",
    "volume": "Float64",
    "quote_volume": "Float64",
    "trades_count": "Int64",
    "taker_buy_volume": "Float64",
    "taker_buy_quote_volume": "Float64",
    "interval": "String",
}

# Semantic column groups.
PRICE_COLUMNS = ["open_price", "high_price", "low_price", "close_price"]
VOLUME_COLUMNS = [
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

# Binance public-data CSV column order (12 columns).
# The "ignore" column is dropped after reading.
BINANCE_CSV_COLUMNS = [
    "open_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "close_time",
    "quote_volume",
    "trades_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
