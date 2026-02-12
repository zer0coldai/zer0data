"""Tests for KlineParser zip file parser."""

import io
import tempfile
import zipfile
from pathlib import Path

import pytest

from zer0data_ingestor.parser.zip_parser import KlineParser


def test_parse_single_zip_file():
    """Test parsing a valid zip file with CSV data."""
    # Sample Binance CSV data (12 columns)
    csv_data = """1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0
1704067260000,42050.00,42200.00,42000.00,42150.00,1200.3,1704067319999,50430000.00,1800,600.15,25200000.00,0
"""

    # Create a temporary zip file with CSV content
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
        zip_path = f.name
        with zipfile.ZipFile(f, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

    try:
        # Parse the zip file
        parser = KlineParser()
        records = list(parser.parse_file(zip_path, "BTCUSDT"))

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
        Path(zip_path).unlink()


def test_parse_nonexistent_file():
    """Test FileNotFoundError is raised for nonexistent file."""
    parser = KlineParser()
    with pytest.raises(FileNotFoundError):
        list(parser.parse_file("/nonexistent/path/file.zip", "BTCUSDT"))


def test_parse_corrupted_zip():
    """Test ValueError is raised for corrupted zip file."""
    # Create a file with invalid zip content
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
        f.write(b"This is not a valid zip file content")
        zip_path = f.name

    try:
        parser = KlineParser()
        with pytest.raises(ValueError, match="Invalid zip file"):
            list(parser.parse_file(zip_path, "BTCUSDT"))
    finally:
        # Clean up temporary file
        Path(zip_path).unlink()


def test_parse_directory():
    """Test parsing all zip files in a directory."""
    # Sample CSV data for different symbols
    btc_csv = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
    eth_csv = "1704067200000,2200.00,2250.00,2180.00,2230.00,5000.0,1704067259999,11150000.00,2000,2500.0,5575000.00,0\n"

    # Create temporary directory with multiple zip files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create BTCUSDT zip file
        btc_zip_path = Path(temp_dir) / "BTCUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(btc_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", btc_csv)

        # Create ETHUSDT zip file
        eth_zip_path = Path(temp_dir) / "ETHUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(eth_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("ETHUSDT-1m-2024-01-01.csv", eth_csv)

        # Parse the directory
        parser = KlineParser()
        results = list(parser.parse_directory(str(temp_dir)))

        # Assert we got records from both files
        assert len(results) == 2

        # Check that we got both symbols
        symbols = [symbol for symbol, _ in results]
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

        # Find and check BTCUSDT record
        btc_record = next(rec for sym, rec in results if sym == "BTCUSDT")
        assert btc_record.symbol == "BTCUSDT"
        assert btc_record.open_price == 42000.00

        # Find and check ETHUSDT record
        eth_record = next(rec for sym, rec in results if sym == "ETHUSDT")
        assert eth_record.symbol == "ETHUSDT"
        assert eth_record.open_price == 2200.00


def test_parse_directory_with_symbols_filter():
    """Test parsing directory with symbols filter."""
    # Sample CSV data
    csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

    # Create temporary directory with multiple zip files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create BTCUSDT zip file
        btc_zip_path = Path(temp_dir) / "BTCUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(btc_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        # Create ETHUSDT zip file
        eth_zip_path = Path(temp_dir) / "ETHUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(eth_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("ETHUSDT-1m-2024-01-01.csv", csv_data)

        # Parse the directory with symbols filter
        parser = KlineParser()
        results = list(parser.parse_directory(str(temp_dir), symbols=["BTCUSDT"]))

        # Assert we only got BTCUSDT records
        assert len(results) == 1
        symbol, record = results[0]
        assert symbol == "BTCUSDT"


def test_parse_empty_zip():
    """Test parsing an empty zip file."""
    # Create an empty zip file
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
        zip_path = f.name
        with zipfile.ZipFile(f, mode="w", compression=zipfile.ZIP_DEFLATED):
            pass  # Empty zip

    try:
        parser = KlineParser()
        records = list(parser.parse_file(zip_path, "BTCUSDT"))
        assert len(records) == 0
    finally:
        Path(zip_path).unlink()


def test_parse_file_with_header_row():
    """Parser should ignore Binance CSV header row."""
    csv_data = """open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0
"""

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
        zip_path = f.name
        with zipfile.ZipFile(f, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

    try:
        parser = KlineParser()
        records = list(parser.parse_file(zip_path, "BTCUSDT"))
        assert len(records) == 1
        assert records[0].open_time == 1704067200000
    finally:
        Path(zip_path).unlink()
