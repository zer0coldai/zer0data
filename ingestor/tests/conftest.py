"""Pytest configuration and fixtures."""

import pytest
import clickhouse_connect


@pytest.fixture
def clickhouse_client():
    """Create a ClickHouse client for testing.

    This fixture creates a client connected to the test ClickHouse instance.
    The client can be used in tests to verify data was written correctly.
    """
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="zer0data",
    )
    yield client
    client.close()
