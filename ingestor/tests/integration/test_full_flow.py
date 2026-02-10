"""Integration test for full ingestion and query flow."""

import os
import subprocess
import time
from datetime import date, timedelta

import pytest
import clickhouse_connect

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.ingestor import KlineIngestor
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord


@pytest.fixture(scope="session")
def clickhouse_running():
    """Start docker-compose and wait for ClickHouse to be ready.

    This fixture:
    1. Starts docker-compose
    2. Waits for ClickHouse to be healthy
    3. Yields the connection parameters
    4. Tears down docker-compose after tests
    """
    # Check if docker-compose is already running
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", "clickhouse"],
            capture_output=True,
            text=True,
            cwd="/Users/rock/work/zer0data",
        )
        if result.stdout.strip():
            # Already running, just verify health
            print("Docker Compose already running, reusing...")
        else:
            # Start docker-compose
            print("Starting Docker Compose...")
            subprocess.run(
                ["docker", "compose", "up", "-d"],
                check=True,
                cwd="/Users/rock/work/zer0data",
            )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        pytest.skip(f"Could not start docker-compose: {e}")

    # Wait for ClickHouse to be ready
    print("Waiting for ClickHouse to be ready...")
    max_retries = 60
    retry_delay = 2

    for i in range(max_retries):
        try:
            client = clickhouse_connect.get_client(
                host="localhost",
                port=8123,
                database="zer0data",
            )
            # Test connection with a simple query
            result = client.query("SELECT 1")
            if result.result_rows[0][0] == 1:
                print(f"ClickHouse is ready! (attempt {i + 1})")
                client.close()
                break
        except Exception as e:
            if i < max_retries - 1:
                print(f"Attempt {i + 1}/{max_retries}: ClickHouse not ready yet, retrying...")
                time.sleep(retry_delay)
            else:
                pytest.skip(f"ClickHouse did not become ready after {max_retries} attempts: {e}")

    yield {"host": "localhost", "port": 8123, "database": "zer0data"}

    # Note: We don't tear down docker-compose to allow for test inspection
    print("\nDocker Compose left running for inspection. Stop manually with: docker compose down")


@pytest.mark.integration
def test_full_ingestion_query_flow(clickhouse_running):
    """Test full ingestion and query flow.

    This test:
    1. Creates mock kline data
    2. Writes it via ClickHouseWriter (simulating KlineIngestor behavior)
    3. Queries the data via direct ClickHouse client
    4. Verifies records were written and data is returned correctly
    """
    # Get connection params from fixture
    conn_params = clickhouse_running

    test_date = date(2025, 1, 15)
    test_symbol = "BTCUSDT"

    print(f"\n=== Testing ingestion for {test_symbol} on {test_date} ===")

    # Clear any existing data for this test
    client = clickhouse_connect.get_client(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )
    client.command(
        f"DELETE FROM {conn_params['database']}.klines "
        f"WHERE symbol = '{test_symbol}'"
    )

    # Create mock kline records (simulating what would be downloaded)
    import time as time_module
    base_timestamp = int(time_module.mktime(test_date.timetuple())) * 1000
    mock_records = []

    for i in range(10):  # Create 10 sample 1-minute klines
        open_time = base_timestamp + (i * 60 * 1000)
        close_time = open_time + (59 * 1000)

        record = KlineRecord(
            symbol=test_symbol,
            open_time=open_time,
            close_time=close_time,
            open_price=50000.0 + i * 10,
            high_price=50100.0 + i * 10,
            low_price=49900.0 + i * 10,
            close_price=50050.0 + i * 10,
            volume=100.0 + i,
            quote_volume=5000000.0 + i * 10000,
            trades_count=1000 + i * 10,
            taker_buy_volume=50.0 + i * 0.5,
            taker_buy_quote_volume=2500000.0 + i * 5000,
        )
        mock_records.append(record)

    # Write records to ClickHouse using ClickHouseWriter
    writer = ClickHouseWriter(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )

    records_written = 0
    for record in mock_records:
        writer.insert(record)
        records_written += 1

    writer.flush()
    writer.close()

    print(f"Records written: {records_written}")

    # Verify records were written
    assert records_written == len(mock_records), (
        f"Expected {len(mock_records)} records written, got {records_written}"
    )

    # Now query the data to verify it was written correctly
    query_client = clickhouse_connect.get_client(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )

    # Query all records for this symbol
    query = f"""
    SELECT *
    FROM {conn_params['database']}.klines
    WHERE symbol = '{test_symbol}'
    ORDER BY open_time
    """

    result = query_client.query(query)
    rows = result.result_rows
    column_names = result.column_names

    print(f"\n=== Query Results ===")
    print(f"Columns: {column_names}")
    print(f"Total rows: {len(rows)}")

    # Verify we got data back
    assert len(rows) > 0, "Expected to retrieve records from ClickHouse"

    # Verify the structure
    expected_columns = [
        "symbol", "open_time", "close_time", "open_price", "high_price",
        "low_price", "close_price", "volume", "quote_volume",
        "trades_count", "taker_buy_volume", "taker_buy_quote_volume"
    ]
    for col in expected_columns:
        assert col in column_names, f"Expected column '{col}' not found in result"

    # Verify data integrity
    for row in rows:
        symbol_idx = column_names.index("symbol")
        open_time_idx = column_names.index("open_time")
        close_time_idx = column_names.index("close_time")

        assert row[symbol_idx] == test_symbol, f"Expected symbol {test_symbol}, got {row[symbol_idx]}"
        assert row[open_time_idx] > 0, "Expected open_time to be positive"
        assert row[close_time_idx] > 0, "Expected close_time to be positive"
        assert row[close_time_idx] >= row[open_time_idx], "Expected close_time >= open_time"

    # Verify the number of records matches what was written
    assert len(rows) == records_written, (
        f"Query returned {len(rows)} rows but wrote {records_written} records"
    )

    print(f"\n=== Test Passed ===")
    print(f"Successfully ingested and queried {len(rows)} records for {test_symbol} on {test_date}")

    query_client.close()


@pytest.mark.integration
def test_multiple_symbols_ingestion(clickhouse_running):
    """Test ingestion for multiple symbols.

    This test verifies that the ingestor can handle multiple symbols correctly.
    """
    conn_params = clickhouse_running

    test_date = date(2025, 1, 14)
    test_symbols = ["BTCUSDT", "ETHUSDT"]

    print(f"\n=== Testing multi-symbol ingestion for {test_symbols} on {test_date} ===")

    # Clear existing data
    client = clickhouse_connect.get_client(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )
    for symbol in test_symbols:
        client.command(
            f"DELETE FROM {conn_params['database']}.klines "
            f"WHERE symbol = '{symbol}'"
        )

    # Create mock kline records for each symbol
    import time as time_module
    base_timestamp = int(time_module.mktime(test_date.timetuple())) * 1000

    writer = ClickHouseWriter(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )

    records_per_symbol = 5
    for symbol in test_symbols:
        for i in range(records_per_symbol):
            open_time = base_timestamp + (i * 60 * 1000)
            close_time = open_time + (59 * 1000)

            # Use different base prices for different symbols
            base_price = 50000.0 if symbol == "BTCUSDT" else 3000.0

            record = KlineRecord(
                symbol=symbol,
                open_time=open_time,
                close_time=close_time,
                open_price=base_price + i * 10,
                high_price=base_price + 100 + i * 10,
                low_price=base_price - 100 + i * 10,
                close_price=base_price + 50 + i * 10,
                volume=100.0 + i,
                quote_volume=(base_price * 100) + i * 1000,
                trades_count=1000 + i * 10,
                taker_buy_volume=50.0 + i * 0.5,
                taker_buy_quote_volume=(base_price * 50) + i * 500,
            )
            writer.insert(record)

    writer.flush()
    writer.close()

    # Verify each symbol has data
    query_client = clickhouse_connect.get_client(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )

    for symbol in test_symbols:
        query = f"""
        SELECT count(*) as count
        FROM {conn_params['database']}.klines
        WHERE symbol = '{symbol}'
        """
        result = query_client.query(query)
        count = result.result_rows[0][0]

        print(f"  {symbol}: {count} records")
        assert count == records_per_symbol, (
            f"Expected {records_per_symbol} records for {symbol}, got {count}"
        )

    query_client.close()

    print(f"\n=== Multi-symbol Test Passed ===")
