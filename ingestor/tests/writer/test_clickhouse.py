"""Tests for ClickHouse writer."""

import pytest
from unittest.mock import MagicMock, patch
from zer0data_ingestor.writer.clickhouse import KlineRecord, ClickHouseWriter


def test_writer_insert_single(clickhouse_client):
    """Test inserting a single kline record."""
    # Clean up before test - create klines_1m table for testing
    clickhouse_client.command("DROP TABLE IF EXISTS klines_1m")
    clickhouse_client.command("""
        CREATE TABLE klines_1m (
            symbol String,
            open_time Int64,
            close_time Int64,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            volume Float64,
            quote_volume Float64,
            trades_count Int64,
            taker_buy_volume Float64,
            taker_buy_quote_volume Float64,
            interval String
        ) ENGINE = MergeTree()
        ORDER BY (symbol, open_time)
    """)

    writer = ClickHouseWriter(host="localhost", port=8123, database="zer0data", table="klines")

    record = KlineRecord(
        symbol="BTCUSDT",
        open_time=1234567890000,
        close_time=1234567899999,
        open_price=50000.0,
        high_price=51000.0,
        low_price=49000.0,
        close_price=50500.0,
        volume=100.0,
        quote_volume=5000000.0,
        trades_count=1000,
        taker_buy_volume=60.0,
        taker_buy_quote_volume=3000000.0,
    )

    writer.insert(record)
    writer.flush()

    # Verify record was inserted
    result = clickhouse_client.query("SELECT * FROM klines_1m WHERE symbol = 'BTCUSDT'")
    assert len(result.result_rows) == 1
    row = result.result_rows[0]
    assert row[0] == "BTCUSDT"  # symbol
    assert row[3] == 50000.0  # open_price

    writer.close()


def test_writer_batch_insert(clickhouse_client):
    """Test batch inserting multiple kline records."""
    # Clean up before test
    clickhouse_client.command("TRUNCATE TABLE klines_1m")

    writer = ClickHouseWriter(host="localhost", port=8123, database="zer0data", table="klines")

    records = [
        KlineRecord(
            symbol="ETHUSDT",
            open_time=1234567890000 + i * 60000,
            close_time=1234567899999 + i * 60000,
            open_price=3000.0 + i,
            high_price=3100.0 + i,
            low_price=2900.0 + i,
            close_price=3050.0 + i,
            volume=50.0 + i,
            quote_volume=150000.0 + i * 1000,
            trades_count=500 + i,
            taker_buy_volume=30.0 + i,
            taker_buy_quote_volume=90000.0 + i * 1000,
        )
        for i in range(10)
    ]

    for record in records:
        writer.insert(record)
    writer.flush()

    # Verify all records were inserted
    result = clickhouse_client.query("SELECT count(*) FROM klines_1m WHERE symbol = 'ETHUSDT'")
    assert result.result_rows[0][0] == 10

    writer.close()


def test_writer_insert_many_flushes_in_chunks():
    """insert_many should flush full batch chunks and keep remainder buffered."""
    with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        writer = ClickHouseWriter(batch_size=2)
        records = [
            KlineRecord(
                symbol="BTCUSDT",
                open_time=1700000000000 + i * 60000,
                close_time=1700000059999 + i * 60000,
                open_price=100.0 + i,
                high_price=101.0 + i,
                low_price=99.0 + i,
                close_price=100.5 + i,
                volume=10.0 + i,
                quote_volume=1000.0 + i,
                trades_count=100 + i,
                taker_buy_volume=5.0 + i,
                taker_buy_quote_volume=500.0 + i,
            )
            for i in range(5)
        ]

        writer.insert_many(records)
        assert mock_client.insert.call_count == 2

        writer.close()
        assert mock_client.insert.call_count == 3

        first_batch_rows = mock_client.insert.call_args_list[0].args[1]
        second_batch_rows = mock_client.insert.call_args_list[1].args[1]
        third_batch_rows = mock_client.insert.call_args_list[2].args[1]

        assert len(first_batch_rows) == 2
        assert len(second_batch_rows) == 2
        assert len(third_batch_rows) == 1

        # Verify interval field is included in data
        for batch_rows in [first_batch_rows, second_batch_rows, third_batch_rows]:
            for row in batch_rows:
                # Last element should be the interval field with default value "1m"
                assert row[-1] == "1m"


class TestDynamicTableName:
    """Tests for dynamic table name based on interval."""

    def test_get_table_name(self):
        """Test _get_table_name returns correct table name for each interval."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client"):
            writer = ClickHouseWriter()

            # Test various intervals
            assert writer._get_table_name("1m") == "klines_1m"
            assert writer._get_table_name("1h") == "klines_1h"
            assert writer._get_table_name("1d") == "klines_1d"
            assert writer._get_table_name("5m") == "klines_5m"
            assert writer._get_table_name("12h") == "klines_12h"

    def test_insert_uses_correct_table(self):
        """Test that insert uses the correct table based on record interval."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            writer = ClickHouseWriter(batch_size=1)

            # Insert record with 1h interval
            record_1h = KlineRecord(
                symbol="BTCUSDT",
                open_time=1700000000000,
                close_time=1700000059999,
                open_price=50000.0,
                high_price=51000.0,
                low_price=49000.0,
                close_price=50500.0,
                volume=100.0,
                quote_volume=5000000.0,
                trades_count=1000,
                taker_buy_volume=60.0,
                taker_buy_quote_volume=3000000.0,
                interval="1h",
            )
            writer.insert(record_1h)

            # Verify the correct table was used
            assert mock_client.insert.call_count == 1
            call_args = mock_client.insert.call_args
            assert call_args[0][0] == "klines_1h"  # First arg is table name

    def test_insert_many_groups_by_interval(self):
        """Test that insert_many groups records by interval."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            writer = ClickHouseWriter(batch_size=10)

            # Create records with different intervals
            records = []
            for i in range(3):
                records.append(
                    KlineRecord(
                        symbol="BTCUSDT",
                        open_time=1700000000000 + i * 60000,
                        close_time=1700000059999 + i * 60000,
                        open_price=50000.0 + i,
                        high_price=51000.0 + i,
                        low_price=49000.0 + i,
                        close_price=50500.0 + i,
                        volume=100.0 + i,
                        quote_volume=5000000.0 + i * 1000,
                        trades_count=1000 + i,
                        taker_buy_volume=60.0 + i,
                        taker_buy_quote_volume=3000000.0 + i * 1000,
                        interval="1m",
                    )
                )
            for i in range(2):
                records.append(
                    KlineRecord(
                        symbol="BTCUSDT",
                        open_time=1700000000000 + i * 3600000,
                        close_time=1700003599999 + i * 3600000,
                        open_price=50000.0 + i,
                        high_price=51000.0 + i,
                        low_price=49000.0 + i,
                        close_price=50500.0 + i,
                        volume=100.0 + i,
                        quote_volume=5000000.0 + i * 1000,
                        trades_count=1000 + i,
                        taker_buy_volume=60.0 + i,
                        taker_buy_quote_volume=3000000.0 + i * 1000,
                        interval="1h",
                    )
                )

            writer.insert_many(records)

            # Verify records are grouped by interval in buffer
            assert len(writer._buffer["1m"]) == 3
            assert len(writer._buffer["1h"]) == 2

    def test_flush_writes_to_multiple_tables(self):
        """Test that flush writes grouped records to correct tables."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            writer = ClickHouseWriter(batch_size=100)

            # Create records with different intervals
            records = []
            for i in range(3):
                records.append(
                    KlineRecord(
                        symbol="BTCUSDT",
                        open_time=1700000000000 + i * 60000,
                        close_time=1700000059999 + i * 60000,
                        open_price=50000.0 + i,
                        high_price=51000.0 + i,
                        low_price=49000.0 + i,
                        close_price=50500.0 + i,
                        volume=100.0 + i,
                        quote_volume=5000000.0 + i * 1000,
                        trades_count=1000 + i,
                        taker_buy_volume=60.0 + i,
                        taker_buy_quote_volume=3000000.0 + i * 1000,
                        interval="1m",
                    )
                )
            for i in range(2):
                records.append(
                    KlineRecord(
                        symbol="ETHUSDT",
                        open_time=1700000000000 + i * 3600000,
                        close_time=1700003599999 + i * 3600000,
                        open_price=3000.0 + i,
                        high_price=3100.0 + i,
                        low_price=2900.0 + i,
                        close_price=3050.0 + i,
                        volume=50.0 + i,
                        quote_volume=150000.0 + i * 1000,
                        trades_count=500 + i,
                        taker_buy_volume=30.0 + i,
                        taker_buy_quote_volume=90000.0 + i * 1000,
                        interval="1h",
                    )
                )

            writer.insert_many(records)
            writer.flush()

            # Verify inserts were made to correct tables
            assert mock_client.insert.call_count == 2

            # Get the table names from calls
            table_names = [call[0][0] for call in mock_client.insert.call_args_list]
            assert "klines_1m" in table_names
            assert "klines_1h" in table_names

            # Verify correct number of records per table
            for call in mock_client.insert.call_args_list:
                table = call[0][0]
                data = call[0][1]
                if table == "klines_1m":
                    assert len(data) == 3
                elif table == "klines_1h":
                    assert len(data) == 2

    def test_insert_many_with_batch_size_flushes_by_interval(self):
        """Test that insert_many with batch_size flushes full batches per interval."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            writer = ClickHouseWriter(batch_size=2)

            # Create 5 records for 1m and 3 records for 1h
            records = []
            for i in range(5):
                records.append(
                    KlineRecord(
                        symbol="BTCUSDT",
                        open_time=1700000000000 + i * 60000,
                        close_time=1700000059999 + i * 60000,
                        open_price=50000.0 + i,
                        high_price=51000.0 + i,
                        low_price=49000.0 + i,
                        close_price=50500.0 + i,
                        volume=100.0 + i,
                        quote_volume=5000000.0 + i * 1000,
                        trades_count=1000 + i,
                        taker_buy_volume=60.0 + i,
                        taker_buy_quote_volume=3000000.0 + i * 1000,
                        interval="1m",
                    )
                )
            for i in range(3):
                records.append(
                    KlineRecord(
                        symbol="ETHUSDT",
                        open_time=1700000000000 + i * 3600000,
                        close_time=1700003599999 + i * 3600000,
                        open_price=3000.0 + i,
                        high_price=3100.0 + i,
                        low_price=2900.0 + i,
                        close_price=3050.0 + i,
                        volume=50.0 + i,
                        quote_volume=150000.0 + i * 1000,
                        trades_count=500 + i,
                        taker_buy_volume=30.0 + i,
                        taker_buy_quote_volume=90000.0 + i * 1000,
                        interval="1h",
                    )
                )

            writer.insert_many(records)

            # Should flush 2 batches of 1m (2+2) and 1 batch of 1h (2)
            # 1m: 5 records -> 2 batches of 2, leaving 1 in buffer
            # 1h: 3 records -> 1 batch of 2, leaving 1 in buffer
            assert mock_client.insert.call_count == 3

            # Check table names
            table_names = [call[0][0] for call in mock_client.insert.call_args_list]
            assert table_names.count("klines_1m") == 2
            assert table_names.count("klines_1h") == 1

            # Flush remaining
            writer.flush()
            assert mock_client.insert.call_count == 5  # 3 + 2 more


class TestAutoCreateTable:
    """Tests for automatic table creation."""

    def test_table_exists_returns_true_for_existing_table(self):
        """Test _table_exists returns True for existing tables."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Mock the query result - table exists (EXISTS TABLE returns (1,))
            mock_client.query.return_value.result_rows = [(1,)]

            writer = ClickHouseWriter()

            result = writer._table_exists("klines_1m")
            assert result is True

            # Verify correct query was made
            mock_client.query.assert_called_once()
            call_args = mock_client.query.call_args[0][0]
            assert "EXISTS TABLE" in call_args
            assert "klines_1m" in call_args

    def test_table_exists_returns_false_for_nonexistent_table(self):
        """Test _table_exists returns False for non-existent tables."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Mock the query result - table doesn't exist (EXISTS TABLE returns (0,))
            mock_client.query.return_value.result_rows = [(0,)]

            writer = ClickHouseWriter()

            result = writer._table_exists("klines_1h")
            assert result is False

    def test_create_table_creates_table_with_correct_schema(self):
        """Test _create_table creates table with correct schema."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            writer = ClickHouseWriter()

            writer._create_table("klines_5m")

            # Verify command was called
            mock_client.command.assert_called_once()
            call_args = mock_client.command.call_args[0][0]

            # Verify it's a CREATE TABLE statement
            assert "CREATE TABLE" in call_args
            assert "klines_5m" in call_args

            # Verify all columns are present
            assert "symbol String" in call_args
            assert "open_time Int64" in call_args
            assert "close_time Int64" in call_args
            assert "open_price Float64" in call_args
            assert "high_price Float64" in call_args
            assert "low_price Float64" in call_args
            assert "close_price Float64" in call_args
            assert "volume Float64" in call_args
            assert "quote_volume Float64" in call_args
            assert "trades_count Int64" in call_args
            assert "taker_buy_volume Float64" in call_args
            assert "taker_buy_quote_volume Float64" in call_args
            assert "interval String" in call_args

            # Verify engine and order by
            assert "ENGINE = MergeTree()" in call_args
            assert "ORDER BY" in call_args
            assert "symbol" in call_args
            assert "open_time" in call_args

    def test_ensure_table_exists_creates_table_if_not_exists(self):
        """Test _ensure_table_exists creates table when it doesn't exist."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Table doesn't exist
            mock_client.query.return_value.result_rows = []

            writer = ClickHouseWriter()

            writer._ensure_table_exists("klines_15m")

            # Should check if table exists
            mock_client.query.assert_called_once()
            # Should create the table
            mock_client.command.assert_called_once()

    def test_ensure_table_exists_skips_if_table_exists(self):
        """Test _ensure_table_exists skips creation when table exists."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Table exists (EXISTS TABLE returns (1,))
            mock_client.query.return_value.result_rows = [(1,)]

            writer = ClickHouseWriter()

            writer._ensure_table_exists("klines_1d")

            # Should check if table exists
            mock_client.query.assert_called_once()
            # Should NOT create the table
            mock_client.command.assert_not_called()

    def test_flush_calls_ensure_table_exists_for_each_interval(self):
        """Test flush ensures tables exist before writing."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Tables don't exist
            mock_client.query.return_value.result_rows = []

            writer = ClickHouseWriter(batch_size=100)

            # Create records with different intervals
            records = []
            for i in range(2):
                records.append(
                    KlineRecord(
                        symbol="BTCUSDT",
                        open_time=1700000000000 + i * 60000,
                        close_time=1700000059999 + i * 60000,
                        open_price=50000.0 + i,
                        high_price=51000.0 + i,
                        low_price=49000.0 + i,
                        close_price=50500.0 + i,
                        volume=100.0 + i,
                        quote_volume=5000000.0 + i * 1000,
                        trades_count=1000 + i,
                        taker_buy_volume=60.0 + i,
                        taker_buy_quote_volume=3000000.0 + i * 1000,
                        interval="1m",
                    )
                )
            for i in range(2):
                records.append(
                    KlineRecord(
                        symbol="ETHUSDT",
                        open_time=1700000000000 + i * 3600000,
                        close_time=1700003599999 + i * 3600000,
                        open_price=3000.0 + i,
                        high_price=3100.0 + i,
                        low_price=2900.0 + i,
                        close_price=3050.0 + i,
                        volume=50.0 + i,
                        quote_volume=150000.0 + i * 1000,
                        trades_count=500 + i,
                        taker_buy_volume=30.0 + i,
                        taker_buy_quote_volume=90000.0 + i * 1000,
                        interval="1h",
                    )
                )

            writer.insert_many(records)
            writer.flush()

            # Verify ensure_table_exists was called for both intervals
            # We should have 2 calls to query (checking existence)
            # and 2 calls to command (creating tables)
            assert mock_client.query.call_count == 2
            assert mock_client.command.call_count == 2

    def test_flush_with_existing_table_skips_creation(self):
        """Test flush skips table creation when table already exists."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Table exists (EXISTS TABLE returns (1,))
            mock_client.query.return_value.result_rows = [(1,)]

            writer = ClickHouseWriter(batch_size=100)

            records = [
                KlineRecord(
                    symbol="BTCUSDT",
                    open_time=1700000000000,
                    close_time=1700000059999,
                    open_price=50000.0,
                    high_price=51000.0,
                    low_price=49000.0,
                    close_price=50500.0,
                    volume=100.0,
                    quote_volume=5000000.0,
                    trades_count=1000,
                    taker_buy_volume=60.0,
                    taker_buy_quote_volume=3000000.0,
                    interval="1m",
                )
            ]

            writer.insert_many(records)
            writer.flush()

            # Should check existence but not create
            mock_client.query.assert_called_once()
            mock_client.command.assert_not_called()

    def test_ensure_table_exists_for_multiple_intervals(self):
        """Test _ensure_table_exists works for various interval formats."""
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # Tables don't exist
            mock_client.query.return_value.result_rows = []

            writer = ClickHouseWriter()

            # Test various intervals
            intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
            for interval in intervals:
                table_name = f"klines_{interval}"
                writer._ensure_table_exists(table_name)

            # Verify each table was checked and created
            assert mock_client.query.call_count == len(intervals)
            assert mock_client.command.call_count == len(intervals)
