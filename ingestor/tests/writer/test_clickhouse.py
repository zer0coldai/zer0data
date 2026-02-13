"""Tests for ClickHouse writer."""

import pytest
from unittest.mock import MagicMock, patch
from zer0data_ingestor.writer.clickhouse import KlineRecord, ClickHouseWriter


def test_writer_insert_single(clickhouse_client):
    """Test inserting a single kline record."""
    # Clean up before test
    clickhouse_client.command("TRUNCATE TABLE klines")

    writer = ClickHouseWriter(host="localhost", port=8123, database="zer0data")

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

    # Verify record has default interval value
    assert record.interval == "1m"

    writer.insert(record)
    writer.flush()

    # Verify record was inserted
    result = clickhouse_client.query("SELECT * FROM klines WHERE symbol = 'BTCUSDT'")
    assert len(result.result_rows) == 1
    row = result.result_rows[0]
    assert row[0] == "BTCUSDT"  # symbol
    assert row[3] == 50000.0  # open_price

    writer.close()


def test_writer_batch_insert(clickhouse_client):
    """Test batch inserting multiple kline records."""
    # Clean up before test
    clickhouse_client.command("TRUNCATE TABLE klines")

    writer = ClickHouseWriter(host="localhost", port=8123, database="zer0data")

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

    # Verify all records have default interval value
    for record in records:
        assert record.interval == "1m"

    for record in records:
        writer.insert(record)
    writer.flush()

    # Verify all records were inserted
    result = clickhouse_client.query("SELECT count(*) FROM klines WHERE symbol = 'ETHUSDT'")
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
