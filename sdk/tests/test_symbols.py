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
        result_rows=[["BTCUSDT", 1569398400000, 4133404800000, "COIN", "TRADING"]],
        column_names=["symbol", "onboardDate", "deliveryDate", "underlyingType", "status"],
    )

    class _MockClient:
        def query(self, sql):
            captured["sql"] = sql
            return mock_result

    service = SymbolService(_MockClient(), "zer0data")
    result = service.query(market="um")

    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["symbol", "onboardDate", "deliveryDate", "underlyingType", "status"]
    assert result.height == 1
    assert result["symbol"][0] == "BTCUSDT"
    assert result["status"][0] == "TRADING"
    assert "FROM zer0data.raw_exchange_info" in captured["sql"]
    assert "WHERE market = 'um'" in captured["sql"]
    assert "AS onboardDate" in captured["sql"]
    assert "AS deliveryDate" in captured["sql"]
    assert "AS underlyingType" in captured["sql"]
    assert "AS status" in captured["sql"]


def test_query_symbols_rejects_invalid_market():
    """SymbolService should reject invalid market values."""
    from zer0data.symbols import SymbolService

    service = SymbolService(SimpleNamespace(query=lambda _: None), "zer0data")
    with pytest.raises(ValueError, match="invalid market"):
        service.query(market="um;DROP TABLE raw_exchange_info")
