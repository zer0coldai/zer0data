"""Tests for KlineParser zip file parser."""

import io
import tempfile
import zipfile
from pathlib import Path

import pytest

from zer0data_ingestor.parser.zip_parser import (
    KlineParser,
    extract_interval_from_filename,
)


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
        assert record1.taker_buy_quote_volume == 21000000.0
        assert record1.interval == "1m"

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
        assert record2.taker_buy_quote_volume == 25200000.0
        assert record2.interval == "1m"
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


class TestExtractIntervalFromFilename:
    """Tests for extract_interval_from_filename function."""

    def test_extract_interval_1m(self):
        """Test extracting 1m interval from filename."""
        interval = extract_interval_from_filename("BTCUSDT-1m-2024-01-01.zip")
        assert interval == "1m"

    def test_extract_interval_1h(self):
        """Test extracting 1h interval from filename."""
        interval = extract_interval_from_filename("BTCUSDT-1h-2024-01-01.zip")
        assert interval == "1h"

    def test_extract_interval_1d(self):
        """Test extracting 1d interval from filename."""
        interval = extract_interval_from_filename("ETHUSDT-1d-2024-01-01.zip")
        assert interval == "1d"

    def test_extract_interval_various_intervals(self):
        """Test extracting various valid intervals from filename."""
        test_cases = [
            ("BTCUSDT-3m-2024-01-01.zip", "3m"),
            ("BTCUSDT-5m-2024-01-01.zip", "5m"),
            ("BTCUSDT-15m-2024-01-01.zip", "15m"),
            ("BTCUSDT-30m-2024-01-01.zip", "30m"),
            ("BTCUSDT-2h-2024-01-01.zip", "2h"),
            ("BTCUSDT-4h-2024-01-01.zip", "4h"),
            ("BTCUSDT-6h-2024-01-01.zip", "6h"),
            ("BTCUSDT-8h-2024-01-01.zip", "8h"),
            ("BTCUSDT-12h-2024-01-01.zip", "12h"),
        ]
        for filename, expected_interval in test_cases:
            assert extract_interval_from_filename(filename) == expected_interval

    def test_extract_interval_malformed_filename(self):
        """Test extracting interval from malformed filename returns default."""
        # Missing parts
        assert extract_interval_from_filename("BTCUSDT.zip") == "1m"
        assert extract_interval_from_filename("BTCUSDT-1m.zip") == "1m"
        assert extract_interval_from_filename("random.txt") == "1m"

    def test_extract_interval_path_with_directory(self):
        """Test extracting interval from full path."""
        interval = extract_interval_from_filename("/path/to/BTCUSDT-1h-2024-01-01.zip")
        assert interval == "1h"


class TestParseFileWithInterval:
    """Tests for parse_file with interval extraction."""

    def test_parse_file_extracts_interval_from_filename(self):
        """Test that parse_file extracts and uses interval from filename."""
        csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
            zip_path = f.name
            # Create file with 1h interval in name
            original_path = Path(zip_path)
            named_path = original_path.parent / "BTCUSDT-1h-2024-01-01.zip"
            original_path.rename(named_path)
            zip_path = str(named_path)

            with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("BTCUSDT-1h-2024-01-01.csv", csv_data)

        try:
            parser = KlineParser()
            records = list(parser.parse_file(zip_path, "BTCUSDT"))
            assert len(records) == 1
            assert records[0].interval == "1h"
        finally:
            Path(zip_path).unlink()

    def test_parse_file_with_explicit_interval(self):
        """Test that parse_file uses explicit interval parameter if provided."""
        csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
            zip_path = f.name
            # Create file with 1m interval in name
            original_path = Path(zip_path)
            named_path = original_path.parent / "BTCUSDT-1m-2024-01-01.zip"
            original_path.rename(named_path)
            zip_path = str(named_path)

            with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        try:
            parser = KlineParser()
            # Override with explicit interval
            records = list(parser.parse_file(zip_path, "BTCUSDT", interval="5m"))
            assert len(records) == 1
            assert records[0].interval == "5m"
        finally:
            Path(zip_path).unlink()


class TestParseDirectoryWithIntervalsFilter:
    """Tests for parse_directory with intervals filter."""

    def test_parse_directory_filters_by_interval(self):
        """Test parsing directory with intervals filter."""
        csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create 1m interval file
            m1_zip_path = Path(temp_dir) / "BTCUSDT-1m-2024-01-01.zip"
            with zipfile.ZipFile(m1_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

            # Create 1h interval file
            h1_zip_path = Path(temp_dir) / "BTCUSDT-1h-2024-01-01.zip"
            with zipfile.ZipFile(h1_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("BTCUSDT-1h-2024-01-01.csv", csv_data)

            # Parse with intervals filter
            parser = KlineParser()
            results = list(parser.parse_directory(str(temp_dir), intervals=["1h"]))

            # Should only get 1h records
            assert len(results) == 1
            _, record = results[0]
            assert record.interval == "1h"

    def test_parse_directory_with_multiple_intervals(self):
        """Test parsing directory with multiple intervals filter."""
        csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple interval files
            for interval in ["1m", "1h", "1d"]:
                zip_path = Path(temp_dir) / f"BTCUSDT-{interval}-2024-01-01.zip"
                with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(f"BTCUSDT-{interval}-2024-01-01.csv", csv_data)

            # Parse with multiple intervals filter
            parser = KlineParser()
            results = list(parser.parse_directory(str(temp_dir), intervals=["1m", "1h"]))

            # Should only get 1m and 1h records (not 1d)
            assert len(results) == 2
            intervals_in_result = {record.interval for _, record in results}
            assert intervals_in_result == {"1m", "1h"}

    def test_parse_directory_extracts_intervals_from_filenames(self):
        """Test that parse_directory extracts interval from each filename."""
        csv_data = "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create files with different intervals
            h1_zip_path = Path(temp_dir) / "BTCUSDT-1h-2024-01-01.zip"
            with zipfile.ZipFile(h1_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("BTCUSDT-1h-2024-01-01.csv", csv_data)

            d1_zip_path = Path(temp_dir) / "ETHUSDT-1d-2024-01-01.zip"
            with zipfile.ZipFile(d1_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("ETHUSDT-1d-2024-01-01.csv", csv_data)

            # Parse without intervals filter
            parser = KlineParser()
            results = list(parser.parse_directory(str(temp_dir)))

            # Check that intervals are correctly extracted
            assert len(results) == 2
            for symbol, record in results:
                if symbol == "BTCUSDT":
                    assert record.interval == "1h"
                elif symbol == "ETHUSDT":
                    assert record.interval == "1d"
