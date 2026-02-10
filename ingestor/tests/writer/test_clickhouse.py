"""Tests for ClickHouse writer."""

import pytest
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

    for record in records:
        writer.insert(record)
    writer.flush()

    # Verify all records were inserted
    result = clickhouse_client.query("SELECT count(*) FROM klines WHERE symbol = 'ETHUSDT'")
    assert result.result_rows[0][0] == 10

    writer.close()
