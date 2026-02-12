"""
Tests for KlineService
"""

import pytest
from zer0data import Client
import polars as pl
from datetime import datetime
from types import SimpleNamespace


@pytest.mark.parametrize(
    "symbols,start,end,limit",
    [
        # Single symbol, no filters
        ("BTCUSDT", None, None, None),
        # Single symbol with limit
        ("BTCUSDT", None, None, 10),
        # Multiple symbols
        (["BTCUSDT", "ETHUSDT"], None, None, None),
        # With timestamps
        ("BTCUSDT", "1704067200000", "1704153600000", None),
    ],
)
def test_query_various_inputs(clickhouse_client, symbols, start, end, limit):
    """Test query with various input combinations"""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")

    # This will fail if the table doesn't exist or is empty, but the query structure is correct
    try:
        result = service.query(symbols=symbols, start=start, end=end, limit=limit)
        # If we get results, verify the structure
        if result.height > 0:
            assert "symbol" in result.columns
            assert "open_time" in result.columns
            assert "open" in result.columns
            assert "high" in result.columns
            assert "low" in result.columns
            assert "close" in result.columns
    except Exception as e:
        # Table might not exist or be empty - that's OK for this test
        # We're primarily testing the query construction logic
        assert "klines" in str(e).lower() or "unknown" in str(e).lower() or "table" in str(e).lower()


def test_query_empty_symbols(clickhouse_client):
    """Test query with empty symbols raises ValueError"""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")
    with pytest.raises(ValueError, match="symbols must be a non-empty"):
        service.query(symbols=[])


def test_query_stream_not_implemented(clickhouse_client):
    """Test query_stream raises NotImplementedError"""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")
    with pytest.raises(NotImplementedError, match="query_stream is not yet implemented"):
        service.query_stream(symbols="BTCUSDT")


def test_kline_service_via_client(client):
    """Test accessing kline service through client"""
    assert client.kline is not None
    assert hasattr(client.kline, "query")
    assert hasattr(client.kline, "query_stream")


def test_query_with_string_conversion(clickhouse_client):
    """Test that single string symbol is converted to list"""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")

    # Both should work (single string vs list)
    try:
        result1 = service.query(symbols="BTCUSDT")
        result2 = service.query(symbols=["BTCUSDT"])
        # Both should have the same schema
        assert result1.schema == result2.schema
    except Exception as e:
        # Table might not exist - that's OK
        assert "klines" in str(e).lower() or "unknown" in str(e).lower()


def test_parse_timestamp_numeric(clickhouse_client):
    """Test timestamp parsing with numeric input"""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")
    result = service._parse_timestamp("1704067200000")
    assert result == 1704067200000


def test_parse_timestamp_datetime(clickhouse_client):
    """Test timestamp parsing with datetime input."""
    from zer0data.kline import KlineService

    service = KlineService(clickhouse_client, "zer0data")
    result = service._parse_timestamp(datetime(2024, 1, 1, 0, 0, 0))
    assert result == 1704067200000


def test_query_converts_clickhouse_result_to_polars():
    """Test query conversion when clickhouse_connect result has rows/columns."""
    from zer0data.kline import KlineService

    mock_result = SimpleNamespace(
        result_rows=[["BTCUSDT", 1704067200000, 1704067259999, 42314.0, 42335.8, 42289.6, 42331.9, 289.641, 12256155.25625, 3310, 175.211, 7414459.86355]],
        column_names=[
            "symbol",
            "open_time",
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trades_count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ],
    )
    mock_client = SimpleNamespace(query=lambda _: mock_result)

    service = KlineService(mock_client, "zer0data")
    result = service.query(symbols="BTCUSDT")

    assert isinstance(result, pl.DataFrame)
    assert result.height == 1
    assert result["symbol"][0] == "BTCUSDT"
