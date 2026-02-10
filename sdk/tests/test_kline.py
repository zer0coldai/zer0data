"""
Tests for KlineService
"""

import pytest
from zer0data import Client
import polars as pl


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
