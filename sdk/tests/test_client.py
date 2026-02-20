"""
Tests for zer0data Client
"""

import pytest
import polars as pl
from zer0data import Client, ClientConfig


def test_client_config_defaults():
    """Test ClientConfig with default values"""
    config = ClientConfig()
    assert config.host == "localhost"
    assert config.port == 8123
    assert config.username == "default"
    assert config.password == ""
    assert config.database == "zer0data"


def test_client_config_custom():
    """Test ClientConfig with custom values"""
    config = ClientConfig(
        host="custom-host",
        port=9000,
        username="admin",
        password="secret",
        database="custom_db",
    )
    assert config.host == "custom-host"
    assert config.port == 9000
    assert config.username == "admin"
    assert config.password == "secret"
    assert config.database == "custom_db"


def test_client_initialization(client_config):
    """Test Client initialization"""
    client = Client(
        host=client_config.host,
        port=client_config.port,
        username=client_config.username,
        password=client_config.password,
        database=client_config.database,
    )
    assert client.config == client_config
    assert client._kline is None
    client.close()


def test_client_default_initialization():
    """Test Client initialization with defaults"""
    client = Client()
    assert client.config.host == "localhost"
    assert client.config.port == 8123
    assert client.config.username == "default"
    assert client.config.password == ""
    assert client.config.database == "zer0data"
    client.close()


def test_kline_property(client):
    """Test kline property creates KlineService"""
    assert client._kline is None
    kline_service = client.kline
    assert client._kline is not None
    assert kline_service is client._kline
    # Calling again should return the same instance
    assert client.kline is kline_service


def test_client_context_manager():
    """Test Client as context manager"""
    with Client() as client:
        assert client is not None
        assert client.config.database == "zer0data"
    # Connection should be closed after exiting context
    # We can't directly test this without mocking, but we can verify no exception is raised


def test_client_close():
    """Test Client close method"""
    client = Client()
    # Should not raise any exception
    client.close()
    # Calling close again should not raise exception
    client.close()


def test_client_get_klines_delegates_to_kline_service(monkeypatch):
    """Client should provide a direct kline query entrypoint."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    client = Client()
    calls = {}

    def _fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return "mock-result"

    monkeypatch.setattr(client.kline, "query", _fake_query)

    result = client.get_klines(
        symbols=["BTCUSDT"],
        interval="1h",
        start="2025-01-01",
        end="2025-01-02",
        limit=10,
    )

    assert result == "mock-result"
    assert calls["kwargs"] == {
        "symbols": ["BTCUSDT"],
        "interval": "1h",
        "start": "2025-01-01",
        "end": "2025-01-02",
        "limit": 10,
    }
    client.close()


def test_client_get_symbols_delegates_to_symbols_service(monkeypatch):
    """Client should provide a direct symbols query entrypoint."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    client = Client()
    calls = {}

    def _fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return "mock-symbols-result"

    monkeypatch.setattr(client.symbols, "query", _fake_query)

    result = client.get_symbols(market="um")

    assert result == "mock-symbols-result"
    assert calls["kwargs"] == {"market": "um"}
    client.close()


def test_client_get_symbols_delegates_quote_asset(monkeypatch):
    """Client should pass optional quote_asset to SymbolService query."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    client = Client()
    calls = {}

    def _fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return "mock-symbols-result"

    monkeypatch.setattr(client.symbols, "query", _fake_query)

    result = client.get_symbols(market="um", quote_asset="USDT")

    assert result == "mock-symbols-result"
    assert calls["kwargs"] == {"market": "um", "quote_asset": "USDT"}
    client.close()


def test_client_get_symbols_delegates_exclude_stable_base(monkeypatch):
    """Client should pass optional exclude_stable_base to SymbolService query."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    client = Client()
    calls = {}

    def _fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return "mock-symbols-result"

    monkeypatch.setattr(client.symbols, "query", _fake_query)

    result = client.get_symbols(market="um", quote_asset="USDT", exclude_stable_base=True)

    assert result == "mock-symbols-result"
    assert calls["kwargs"] == {
        "market": "um",
        "quote_asset": "USDT",
        "exclude_stable_base": True,
    }
    client.close()


def test_client_reads_clickhouse_config_from_env(monkeypatch):
    """Client() should read clickhouse config from environment variables."""
    from zer0data import client as client_module

    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_HOST", "env-host")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_PORT", "9001")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_DATABASE", "env-db")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_USERNAME", "env-user")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_PASSWORD", "env-pass")

    captured = {}

    class _MockCHClient:
        def close(self):
            return None

    def _fake_get_client(**kwargs):
        captured["kwargs"] = kwargs
        return _MockCHClient()

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", _fake_get_client)

    client = Client()

    assert client.config.host == "env-host"
    assert client.config.port == 9001
    assert client.config.database == "env-db"
    assert client.config.username == "env-user"
    assert client.config.password == "env-pass"
    assert captured["kwargs"] == {
        "host": "env-host",
        "port": 9001,
        "database": "env-db",
        "username": "env-user",
        "password": "env-pass",
    }

    client.close()


def test_client_from_env_factory(monkeypatch):
    """Client.from_env() should construct a client using environment variables."""
    from zer0data import client as client_module

    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_HOST", "factory-host")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_PORT", "9002")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_DATABASE", "factory-db")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_USERNAME", "factory-user")
    monkeypatch.setenv("ZER0DATA_CLICKHOUSE_PASSWORD", "factory-pass")

    captured = {}

    class _MockCHClient:
        def close(self):
            return None

    def _fake_get_client(**kwargs):
        captured["kwargs"] = kwargs
        return _MockCHClient()

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", _fake_get_client)

    client = Client.from_env()

    assert client.config.host == "factory-host"
    assert client.config.port == 9002
    assert client.config.database == "factory-db"
    assert client.config.username == "factory-user"
    assert client.config.password == "factory-pass"
    assert captured["kwargs"] == {
        "host": "factory-host",
        "port": 9002,
        "database": "factory-db",
        "username": "factory-user",
        "password": "factory-pass",
    }

    client.close()


# FactorService tests


def test_factor_write_success_returns_row_count(monkeypatch):
    """FactorService.write should validate and write long-format factor rows."""
    from zer0data import factor as factor_module

    class _MockCHClient:
        def __init__(self):
            self.calls = []

        def close(self):
            return None

        def insert(self, table, data, column_names=None):
            self.calls.append((table, data, column_names))

    mock_client = _MockCHClient()
    monkeypatch.setattr(
        factor_module.clickhouse_connect, "get_client", lambda **_: mock_client
    )

    from zer0data import Client

    client = Client()
    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "datetime": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "factor_name": ["price_usd", "price_usd"],
            "factor_value": [42500.5, 2250.75],
        }
    )

    written = client.write_factors(rows, source="sdk-test")

    assert written == 2
    assert len(mock_client.calls) == 1
    table, data, columns = mock_client.calls[0]
    assert table == "zer0data.factors"
    assert columns == [
        "symbol",
        "datetime",
        "factor_name",
        "factor_value",
        "source",
        "update_time",
    ]
    assert len(data) == 2
    assert data[0][0] == "BTCUSDT"
    assert data[0][2] == "price_usd"
    assert data[0][4] == "sdk-test"
    client.close()


def test_factor_write_missing_required_columns_raises_error(monkeypatch):
    """FactorService.write should reject missing required columns."""
    from zer0data import factor as factor_module

    class _MockCHClient:
        def close(self):
            return None

        def insert(self, table, data, column_names=None):
            return None

    monkeypatch.setattr(
        factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient()
    )

    from zer0data import Client

    client = Client()
    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "datetime": ["2024-01-01T00:00:00Z"],
            "factor_name": ["price_usd"],
        }
    )

    with pytest.raises(ValueError, match="missing required columns"):
        client.write_factors(rows)

    client.close()


def test_factor_write_empty_dataframe_raises_error(monkeypatch):
    """FactorService.write should reject empty input."""
    from zer0data import factor as factor_module

    class _MockCHClient:
        def close(self):
            return None

        def insert(self, table, data, column_names=None):
            return None

    monkeypatch.setattr(
        factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient()
    )

    from zer0data import Client

    client = Client()

    with pytest.raises(ValueError, match="must be a non-empty DataFrame"):
        client.write_factors(
            pl.DataFrame(
                schema={
                    "symbol": pl.String,
                    "datetime": pl.String,
                    "factor_name": pl.String,
                    "factor_value": pl.Float64,
                }
            )
        )

    client.close()


def test_factor_write_accepts_pandas_dataframe(monkeypatch):
    """FactorService.write should accept pandas DataFrame input."""
    pd = pytest.importorskip("pandas")
    from zer0data import factor as factor_module

    class _MockCHClient:
        def __init__(self):
            self.calls = []

        def close(self):
            return None

        def insert(self, table, data, column_names=None):
            self.calls.append((table, data, column_names))

    mock_client = _MockCHClient()
    monkeypatch.setattr(
        factor_module.clickhouse_connect, "get_client", lambda **_: mock_client
    )

    from zer0data import Client

    client = Client()
    rows = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "datetime": ["2024-01-01T00:00:00Z"],
            "factor_name": ["price_usd"],
            "factor_value": [42500.5],
        }
    )

    written = client.write_factors(rows, source="sdk-pandas")

    assert written == 1
    assert len(mock_client.calls) == 1
    table, data, columns = mock_client.calls[0]
    assert table == "zer0data.factors"
    assert columns == [
        "symbol",
        "datetime",
        "factor_name",
        "factor_value",
        "source",
        "update_time",
    ]
    assert data[0][0] == "BTCUSDT"
    assert data[0][4] == "sdk-pandas"
    client.close()


def test_factor_write_drops_invalid_factor_values(monkeypatch):
    """FactorService.write should drop NaN/inf/invalid factor_value rows by default."""
    from zer0data import factor as factor_module

    class _MockCHClient:
        def __init__(self):
            self.calls = []

        def close(self):
            return None

        def insert(self, table, data, column_names=None):
            self.calls.append((table, data, column_names))

    mock_client = _MockCHClient()
    monkeypatch.setattr(
        factor_module.clickhouse_connect, "get_client", lambda **_: mock_client
    )

    from zer0data import Client

    client = Client()
    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "datetime": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T02:00:00Z",
                "2024-01-01T03:00:00Z",
            ],
            "factor_name": ["price_usd", "price_usd", "price_usd", "price_usd"],
            "factor_value": [None, float("nan"), float("inf"), 42500.5],
        }
    )

    written = client.write_factors(rows, source="sdk-clean")

    assert written == 1
    assert len(mock_client.calls) == 1
    table, data, _ = mock_client.calls[0]
    assert table == "zer0data.factors"
    assert len(data) == 1
    assert data[0][3] == 42500.5
    client.close()


def test_factor_query_long_format(monkeypatch):
    """Test FactorService.query with long format (default)."""
    from zer0data import factor as factor_module

    class _MockQueryResult:
        result_rows = [
            ("BTCUSDT", 1704067200000, "price_usd", 42500.50),
            ("BTCUSDT", 1704067200000, "volume", 1234567.89),
        ]
        column_names = ["symbol", "datetime", "factor_name", "factor_value"]

    class _MockCHClient:
        def close(self):
            return None

        def query(self, sql):
            return _MockQueryResult()

    monkeypatch.setattr(factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    # Access factors property to initialize the service
    _ = client.factors

    result = client.get_factors(
        symbols="BTCUSDT", factor_names="price_usd", format="long"
    )

    assert result.shape == (2, 4)
    assert result.columns == ["symbol", "datetime", "factor_name", "factor_value"]
    assert result["symbol"][0] == "BTCUSDT"
    assert result["factor_name"][0] == "price_usd"
    client.close()


def test_factor_query_wide_format(monkeypatch):
    """Test FactorService.query with wide format."""
    from zer0data import factor as factor_module

    class _MockQueryResult:
        result_rows = [
            ("BTCUSDT", 1704067200000, "price_usd", 42500.50),
            ("BTCUSDT", 1704067200000, "volume", 1234567.89),
        ]
        column_names = ["symbol", "datetime", "factor_name", "factor_value"]

    class _MockCHClient:
        def close(self):
            return None

        def query(self, sql):
            return _MockQueryResult()

    monkeypatch.setattr(factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    # Access factors property to initialize the service
    _ = client.factors

    result = client.get_factors(
        symbols="BTCUSDT", factor_names=["price_usd", "volume"], format="wide"
    )

    assert result.shape == (1, 4)
    assert "price_usd" in result.columns
    assert "volume" in result.columns
    assert result["symbol"][0] == "BTCUSDT"
    client.close()


def test_factor_query_single_symbol_factor(monkeypatch):
    """Test FactorService.query with single symbol and factor."""
    from zer0data import factor as factor_module

    class _MockQueryResult:
        result_rows = [("BTCUSDT", 1704067200000, "price_usd", 42500.50)]
        column_names = ["symbol", "datetime", "factor_name", "factor_value"]

    class _MockCHClient:
        def close(self):
            return None

        def query(self, sql):
            return _MockQueryResult()

    monkeypatch.setattr(factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    # Access factors property to initialize the service
    _ = client.factors

    result = client.get_factors(symbols="BTCUSDT", factor_names="price_usd")

    assert result.shape == (1, 4)
    assert result["symbol"][0] == "BTCUSDT"
    assert result["factor_name"][0] == "price_usd"
    client.close()


def test_factor_query_multiple_symbols_factors(monkeypatch):
    """Test FactorService.query with multiple symbols and factors."""
    from zer0data import factor as factor_module

    class _MockQueryResult:
        result_rows = [
            ("BTCUSDT", 1704067200000, "price_usd", 42500.50),
            ("ETHUSDT", 1704067200000, "price_usd", 2250.75),
        ]
        column_names = ["symbol", "datetime", "factor_name", "factor_value"]

    class _MockCHClient:
        def close(self):
            return None

        def query(self, sql):
            return _MockQueryResult()

    monkeypatch.setattr(factor_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    # Access factors property to initialize the service
    _ = client.factors

    result = client.get_factors(
        symbols=["BTCUSDT", "ETHUSDT"], factor_names=["price_usd"]
    )

    assert result.shape == (2, 4)
    client.close()


def test_factor_normalize_symbols():
    """Test _normalize_symbols normalizes and deduplicates symbols."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    # Single string
    assert service._normalize_symbols("BTCUSDT") == ["BTCUSDT"]

    # List of symbols
    assert service._normalize_symbols(["BTCUSDT", "ETHUSDT"]) == ["BTCUSDT", "ETHUSDT"]

    # Deduplication
    assert service._normalize_symbols(["BTCUSDT", "BTCUSDT", "ETHUSDT"]) == [
        "BTCUSDT",
        "ETHUSDT",
    ]


def test_factor_normalize_symbols_empty_raises_error():
    """Test _normalize_symbols raises error for empty symbols."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    with pytest.raises(ValueError, match="symbols must be a non-empty"):
        service._normalize_symbols([])

    with pytest.raises(ValueError, match="symbols must be a non-empty"):
        service._normalize_symbols([])


def test_factor_normalize_factor_names():
    """Test _normalize_factor_names normalizes and deduplicates factor_names."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    # Single string
    assert service._normalize_factor_names("price_usd") == ["price_usd"]

    # List of factors
    assert service._normalize_factor_names(["price_usd", "volume"]) == [
        "price_usd",
        "volume",
    ]

    # Deduplication
    assert service._normalize_factor_names(["price_usd", "price_usd", "volume"]) == [
        "price_usd",
        "volume",
    ]


def test_factor_normalize_factor_names_empty_raises_error():
    """Test _normalize_factor_names raises error for empty factor_names."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    with pytest.raises(ValueError, match="factor_names must be a non-empty"):
        service._normalize_factor_names([])

    with pytest.raises(ValueError, match="factor_names must be a non-empty"):
        service._normalize_factor_names([])


def test_factor_validate_format():
    """Test _validate_format validates format parameter."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    # Valid formats
    assert service._validate_format("long") == "long"
    assert service._validate_format("wide") == "wide"
    assert service._validate_format("LONG") == "long"
    assert service._validate_format("WIDE") == "wide"

    # Invalid format
    with pytest.raises(ValueError, match="format must be 'long' or 'wide'"):
        service._validate_format("invalid")


def test_factor_build_where_clause():
    """Test _build_where_clause builds correct SQL WHERE clause."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    service = FactorService(_MockCHClient(), "zer0data")

    # Only symbols and factor_names
    where = service._build_where_clause(["BTCUSDT"], ["price_usd"], None, None)
    assert "symbol IN ('BTCUSDT')" in where
    assert "factor_name IN ('price_usd')" in where

    # With time range
    where = service._build_where_clause(
        ["BTCUSDT", "ETHUSDT"], ["price_usd"], "2024-01-01", "2024-01-02"
    )
    assert "symbol IN ('BTCUSDT', 'ETHUSDT')" in where
    assert "factor_name IN ('price_usd')" in where
    assert "datetime >=" in where
    assert "datetime <=" in where
    assert "datetime >= toDateTime(1704067200, 'UTC')" in where
    assert "datetime <= toDateTime(1704153600, 'UTC')" in where


def test_factor_query_empty_symbols_raises_error(monkeypatch):
    """Test FactorService.query raises error for empty symbols."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(__import__('zer0data.factor', fromlist=['clickhouse_connect']).clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()

    with pytest.raises(ValueError, match="symbols must be a non-empty"):
        client.get_factors(symbols=[], factor_names="price_usd")

    client.close()


def test_factor_query_empty_factor_names_raises_error(monkeypatch):
    """Test FactorService.query raises error for empty factor_names."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(__import__('zer0data.factor', fromlist=['clickhouse_connect']).clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()

    with pytest.raises(ValueError, match="factor_names must be a non-empty"):
        client.get_factors(symbols="BTCUSDT", factor_names=[])

    client.close()


def test_factor_query_invalid_format_raises_error(monkeypatch):
    """Test FactorService.query raises error for invalid format."""
    from zer0data.factor import FactorService

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(__import__('zer0data.factor', fromlist=['clickhouse_connect']).clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()

    with pytest.raises(ValueError, match="format must be 'long' or 'wide'"):
        client.get_factors(symbols="BTCUSDT", factor_names="price_usd", format="invalid")

    client.close()


def test_client_get_factors_delegates_to_factor_service(monkeypatch):
    """Client should provide a direct factor query entrypoint."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    calls = {}

    def _fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return "mock-factors-result"

    monkeypatch.setattr(client.factors, "query", _fake_query)

    result = client.get_factors(
        symbols=["BTCUSDT"],
        factor_names=["price_usd"],
        start="2024-01-01",
        end="2024-01-02",
        format="wide",
    )

    assert result == "mock-factors-result"
    assert calls["kwargs"] == {
        "symbols": ["BTCUSDT"],
        "factor_names": ["price_usd"],
        "start": "2024-01-01",
        "end": "2024-01-02",
        "format": "wide",
    }
    client.close()


def test_client_write_factors_delegates_to_factor_service(monkeypatch):
    """Client should provide a direct factor write entrypoint."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    calls = {}

    def _fake_write(data, source="sdk"):
        calls["data"] = data
        calls["source"] = source
        return 7

    monkeypatch.setattr(client.factors, "write", _fake_write)
    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "datetime": ["2024-01-01T00:00:00Z"],
            "factor_name": ["price_usd"],
            "factor_value": [42500.5],
        }
    )

    written = client.write_factors(rows, source="sdk")

    assert written == 7
    assert calls["data"].shape == (1, 4)
    assert calls["source"] == "sdk"
    client.close()


def test_client_factors_property_creates_service(monkeypatch):
    """Test factors property creates FactorService lazily."""
    from zer0data import client as client_module

    class _MockCHClient:
        def close(self):
            return None

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", lambda **_: _MockCHClient())
    from zer0data import Client

    client = Client()
    assert client._factors is None
    factors_service = client.factors
    assert client._factors is not None
    assert factors_service is client._factors
    # Calling again should return the same instance
    assert client.factors is factors_service
    client.close()
