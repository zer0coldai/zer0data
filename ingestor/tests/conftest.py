"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def clickhouse_client():
    """Create a ClickHouse client for testing.

    This fixture creates a client connected to the test ClickHouse instance.
    Only used by integration tests that require a running database.
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="zer0data",
    )
    yield client
    client.close()
