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
