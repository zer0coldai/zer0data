"""Tests for kline CSV parser."""

import tempfile
from pathlib import Path

import pytest

from zer0data_ingestor.parser.kline import parse_klines_csv, _ms_to_datetime


def test_parse_klines_csv():
    """Test parsing klines from CSV with sample data."""
    # Sample Binance CSV data
    csv_data = """1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0
1704067260000,42050.00,42200.00,42000.00,42150.00,1200.3,1704067319999,50430000.00,1800,600.15,25200000.00,0
"""

    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_data)
        temp_path = f.name

    try:
        # Parse the CSV file
        records = list(parse_klines_csv(temp_path, "BTCUSDT"))

        # Assert we got 2 records
        assert len(records) == 2

        # Check first record
        record1 = records[0]
        assert record1.symbol == "BTCUSDT"
        assert record1.open_time == 1704067200000
        assert record1.close_time == 1704067259999
        assert record1.open_price == 42000.00
        assert record1.high_price == 42100.00
        assert record1.low_price == 41900.00
        assert record1.close_price == 42050.00
        assert record1.volume == 1000.5
        assert record1.quote_volume == 42050000.00
        assert record1.trades_count == 1500
        assert record1.taker_buy_volume == 500.25
        assert record1.taker_buy_quote_volume == 21000000.00

        # Check second record
        record2 = records[1]
        assert record2.symbol == "BTCUSDT"
        assert record2.open_time == 1704067260000
        assert record2.close_time == 1704067319999
        assert record2.open_price == 42050.00
        assert record2.high_price == 42200.00
        assert record2.low_price == 42000.00
        assert record2.close_price == 42150.00
        assert record2.volume == 1200.3
        assert record2.quote_volume == 50430000.00
        assert record2.trades_count == 1800
        assert record2.taker_buy_volume == 600.15
        assert record2.taker_buy_quote_volume == 25200000.00
    finally:
        # Clean up temporary file
        Path(temp_path).unlink()


def test_parse_klines_csv_empty():
    """Test parsing empty CSV file."""
    # Create empty temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = f.name

    try:
        # Parse the empty CSV file
        records = list(parse_klines_csv(temp_path, "BTCUSDT"))

        # Assert we got no records
        assert len(records) == 0
    finally:
        # Clean up temporary file
        Path(temp_path).unlink()


def test_ms_to_datetime():
    """Test milliseconds to datetime conversion."""
    # Test a known timestamp (2024-01-01 00:00:00 UTC)
    ms = 1704067200000
    dt = _ms_to_datetime(ms)
    assert dt.year == 2024
    assert dt.month == 1
    assert dt.day == 1
    assert dt.minute == 0
    assert dt.second == 0
