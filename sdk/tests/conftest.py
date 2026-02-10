"""
Pytest configuration and fixtures
"""

import pytest
import clickhouse_connect
from zer0data import Client, ClientConfig


@pytest.fixture
def clickhouse_client():
    """ClickHouse client fixture"""
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="zer0data",
        username="default",
        password="",
    )
    yield client
    client.close()


@pytest.fixture
def client_config():
    """Client configuration fixture"""
    return ClientConfig(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="zer0data",
    )


@pytest.fixture
def client():
    """zer0data client fixture"""
    client = Client()
    yield client
    client.close()
