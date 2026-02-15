"""Tests for KlineParser zip file parser — DataFrame edition."""

import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from zer0data_ingestor.parser.zip_parser import (
    KlineParser,
    extract_interval_from_filename,
)
from zer0data_ingestor.schema import KLINE_COLUMNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_zip(tmp_dir: str, filename: str, csv_data: str) -> Path:
    """Create a zip file containing one CSV inside *tmp_dir*."""
    zip_path = Path(tmp_dir) / filename
    csv_name = filename.replace(".zip", ".csv")
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_data)
    return zip_path


SAMPLE_ROW = (
    "1704067200000,42000.00,42100.00,41900.00,42050.00,"
    "1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0"
)
SAMPLE_ROW_2 = (
    "1704067260000,42050.00,42200.00,42000.00,42150.00,"
    "1200.3,1704067319999,50430000.00,1800,600.15,25200000.00,0"
)
SAMPLE_CSV = f"{SAMPLE_ROW}\n{SAMPLE_ROW_2}\n"


# ---------------------------------------------------------------------------
# parse_file
# ---------------------------------------------------------------------------

def test_parse_single_zip_file():
    """parse_file returns a DataFrame with correct columns and values."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", SAMPLE_CSV)

        parser = KlineParser()
        df = parser.parse_file(str(zip_path), "BTCUSDT")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        # All expected columns are present.
        for col in KLINE_COLUMNS:
            assert col in df.columns, f"Missing column: {col}"

        row = df.iloc[0]
        assert row["symbol"] == "BTCUSDT"
        assert row["open_time"] == 1704067200000
        assert row["close_time"] == 1704067259999
        assert row["open_price"] == 42000.00
        assert row["high_price"] == 42100.00
        assert row["low_price"] == 41900.00
        assert row["close_price"] == 42050.00
        assert row["volume"] == 1000.5
        assert row["quote_volume"] == 42050000.00
        assert row["trades_count"] == 1500
        assert row["taker_buy_volume"] == 500.25
        assert row["taker_buy_quote_volume"] == 21000000.0
        assert row["interval"] == "1m"


def test_parse_nonexistent_file():
    parser = KlineParser()
    with pytest.raises(FileNotFoundError):
        parser.parse_file("/nonexistent/path/file.zip", "BTCUSDT")


def test_parse_corrupted_zip():
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False) as f:
        f.write(b"This is not a valid zip file content")
        zip_path = f.name

    try:
        parser = KlineParser()
        with pytest.raises(ValueError, match="Invalid zip file"):
            parser.parse_file(zip_path, "BTCUSDT", interval="1m")
    finally:
        Path(zip_path).unlink()


def test_parse_empty_zip():
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = Path(tmp_dir) / "BTCUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(zip_path, mode="w"):
            pass

        parser = KlineParser()
        df = parser.parse_file(str(zip_path), "BTCUSDT")
        assert df.empty


def test_parse_file_with_header_row():
    header = (
        "open_time,open,high,low,close,volume,close_time,"
        "quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore"
    )
    csv_data = f"{header}\n{SAMPLE_ROW}\n"

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", csv_data)

        parser = KlineParser()
        df = parser.parse_file(str(zip_path), "BTCUSDT")
        assert len(df) == 1
        assert df.iloc[0]["open_time"] == 1704067200000


# ---------------------------------------------------------------------------
# parse_file interval handling
# ---------------------------------------------------------------------------

class TestParseFileWithInterval:
    def test_extracts_interval_from_filename(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = _make_zip(tmp_dir, "BTCUSDT-1h-2024-01-01.zip", SAMPLE_ROW + "\n")

            parser = KlineParser()
            df = parser.parse_file(str(zip_path), "BTCUSDT")
            assert len(df) == 1
            assert df.iloc[0]["interval"] == "1h"

    def test_explicit_interval_overrides(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", SAMPLE_ROW + "\n")

            parser = KlineParser()
            df = parser.parse_file(str(zip_path), "BTCUSDT", interval="5m")
            assert df.iloc[0]["interval"] == "5m"


# ---------------------------------------------------------------------------
# parse_directory
# ---------------------------------------------------------------------------

def test_parse_directory():
    btc_csv = SAMPLE_ROW + "\n"
    eth_csv = (
        "1704067200000,2200.00,2250.00,2180.00,2230.00,"
        "5000.0,1704067259999,11150000.00,2000,2500.0,5575000.00,0\n"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", btc_csv)
        _make_zip(tmp_dir, "ETHUSDT-1m-2024-01-01.zip", eth_csv)

        parser = KlineParser()
        results = list(parser.parse_directory(str(tmp_dir)))

        assert len(results) == 2
        symbols = [sym for sym, _, _ in results]
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

        for _, _, df in results:
            assert isinstance(df, pd.DataFrame)
            assert not df.empty


def test_parse_directory_with_symbols_filter():
    csv_data = SAMPLE_ROW + "\n"

    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", csv_data)
        _make_zip(tmp_dir, "ETHUSDT-1m-2024-01-01.zip", csv_data)

        parser = KlineParser()
        results = list(parser.parse_directory(str(tmp_dir), symbols=["BTCUSDT"]))

        assert len(results) == 1
        sym, _, _ = results[0]
        assert sym == "BTCUSDT"


# ---------------------------------------------------------------------------
# extract_interval_from_filename
# ---------------------------------------------------------------------------

class TestExtractIntervalFromFilename:
    def test_extract_1m(self):
        assert extract_interval_from_filename("BTCUSDT-1m-2024-01-01.zip") == "1m"

    def test_extract_1h(self):
        assert extract_interval_from_filename("BTCUSDT-1h-2024-01-01.zip") == "1h"

    def test_extract_1d(self):
        assert extract_interval_from_filename("ETHUSDT-1d-2024-01-01.zip") == "1d"

    def test_extract_various(self):
        cases = [
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
        for filename, expected in cases:
            assert extract_interval_from_filename(filename) == expected

    def test_malformed_filename_raises(self):
        with pytest.raises(ValueError):
            extract_interval_from_filename("BTCUSDT.zip")
        with pytest.raises(ValueError):
            extract_interval_from_filename("random.txt")

    def test_path_with_directory(self):
        assert extract_interval_from_filename("/path/to/BTCUSDT-1h-2024-01-01.zip") == "1h"


class TestParseDirectoryWithIntervalsFilter:
    def test_filters_by_interval(self):
        csv_data = SAMPLE_ROW + "\n"

        with tempfile.TemporaryDirectory() as tmp_dir:
            _make_zip(tmp_dir, "BTCUSDT-1m-2024-01-01.zip", csv_data)
            _make_zip(tmp_dir, "BTCUSDT-1h-2024-01-01.zip", csv_data)

            parser = KlineParser()
            results = list(parser.parse_directory(str(tmp_dir), intervals=["1h"]))

            assert len(results) == 1
            _, interval, df = results[0]
            assert interval == "1h"
            assert df.iloc[0]["interval"] == "1h"

    def test_multiple_intervals(self):
        csv_data = SAMPLE_ROW + "\n"

        with tempfile.TemporaryDirectory() as tmp_dir:
            for iv in ["1m", "1h", "1d"]:
                _make_zip(tmp_dir, f"BTCUSDT-{iv}-2024-01-01.zip", csv_data)

            parser = KlineParser()
            results = list(parser.parse_directory(str(tmp_dir), intervals=["1m", "1h"]))

            assert len(results) == 2
            intervals_in_result = {iv for _, iv, _ in results}
            assert intervals_in_result == {"1m", "1h"}

    def test_extracts_intervals_from_filenames(self):
        csv_data = SAMPLE_ROW + "\n"

        with tempfile.TemporaryDirectory() as tmp_dir:
            _make_zip(tmp_dir, "BTCUSDT-1h-2024-01-01.zip", csv_data)
            _make_zip(tmp_dir, "ETHUSDT-1d-2024-01-01.zip", csv_data)

            parser = KlineParser()
            results = list(parser.parse_directory(str(tmp_dir)))

            assert len(results) == 2
            for sym, interval, df in results:
                if sym == "BTCUSDT":
                    assert interval == "1h"
                    assert df.iloc[0]["interval"] == "1h"
                elif sym == "ETHUSDT":
                    assert interval == "1d"
                    assert df.iloc[0]["interval"] == "1d"
