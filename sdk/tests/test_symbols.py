"""
Tests for SymbolService
"""

from types import SimpleNamespace

import polars as pl
import pytest


def test_query_symbols_from_latest_exchange_info_payload():
    """SymbolService should query latest payload and return required fields."""
    from zer0data.symbols import SymbolService

    captured = {}
    mock_result = SimpleNamespace(
        result_rows=[["BTCUSDT", "USDT", 1569398400000, 4133404800000, "COIN", "TRADING"]],
        column_names=["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"],
    )

    class _MockClient:
        def query(self, sql):
            captured["sql"] = sql
            return mock_result

    service = SymbolService(_MockClient(), "zer0data")
    result = service.query(market="um")

    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"]
    assert result.height == 1
    assert result["symbol"][0] == "BTCUSDT"
    assert result["quoteAsset"][0] == "USDT"
    assert result["status"][0] == "TRADING"
    assert "FROM zer0data.raw_exchange_info" in captured["sql"]
    assert "WHERE market = 'um'" in captured["sql"]
    assert "AS quoteAsset" in captured["sql"]
    assert "AS onboardDate" in captured["sql"]
    assert "AS deliveryDate" in captured["sql"]
    assert "AS underlyingType" in captured["sql"]
    assert "AS status" in captured["sql"]


def test_query_symbols_with_quote_asset_filter():
    """SymbolService should filter by quoteAsset when provided."""
    from zer0data.symbols import SymbolService

    captured = {}
    mock_result = SimpleNamespace(
        result_rows=[["BTCUSDT", "USDT", 1569398400000, 4133404800000, "COIN", "TRADING"]],
        column_names=["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"],
    )

    class _MockClient:
        def query(self, sql):
            captured["sql"] = sql
            return mock_result

    service = SymbolService(_MockClient(), "zer0data")
    result = service.query(market="um", quote_asset="USDT")

    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"]
    assert result.height == 1
    assert result["symbol"][0] == "BTCUSDT"
    assert result["quoteAsset"][0] == "USDT"
    assert "JSONExtractString(symbol_raw, 'quoteAsset') AS quoteAsset" in captured["sql"]
    assert "AND JSONExtractString(symbol_raw, 'quoteAsset') = 'USDT'" in captured["sql"]


def test_query_symbols_with_exclude_stable_base_filter():
    """SymbolService should filter out stable base assets when enabled."""
    from zer0data.symbols import SymbolService

    captured = {}
    mock_result = SimpleNamespace(
        result_rows=[["BTCUSDT", "USDT", 1569398400000, 4133404800000, "COIN", "TRADING"]],
        column_names=["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"],
    )

    class _MockClient:
        def query(self, sql):
            captured["sql"] = sql
            return mock_result

    service = SymbolService(_MockClient(), "zer0data")
    result = service.query(market="um", quote_asset="USDT", exclude_stable_base=True)

    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["symbol", "quoteAsset", "onboardDate", "deliveryDate", "underlyingType", "status"]
    assert result.height == 1
    assert "JSONExtractString(symbol_raw, 'baseAsset') NOT IN" in captured["sql"]
    assert "'USDC'" in captured["sql"]
    assert "'BUSD'" in captured["sql"]
    assert "'FDUSD'" in captured["sql"]


def test_query_symbols_rejects_invalid_market():
    """SymbolService should reject invalid market values."""
    from zer0data.symbols import SymbolService

    service = SymbolService(SimpleNamespace(query=lambda _: None), "zer0data")
    with pytest.raises(ValueError, match="invalid market"):
        service.query(market="um;DROP TABLE raw_exchange_info")


def test_query_symbols_rejects_invalid_quote_asset():
    """SymbolService should reject invalid quote_asset values."""
    from zer0data.symbols import SymbolService

    service = SymbolService(SimpleNamespace(query=lambda _: None), "zer0data")
    with pytest.raises(ValueError, match="invalid quote_asset"):
        service.query(market="um", quote_asset="USDT';DROP TABLE raw_exchange_info;--")
