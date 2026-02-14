"""
Tests for zer0data Client
"""

import pytest
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
