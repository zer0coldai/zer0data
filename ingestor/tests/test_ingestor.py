"""Tests for KlineIngestor."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.ingestor import KlineIngestor, IngestStats


@pytest.fixture
def ingestor_config():
    """Create a test IngestorConfig."""
    return IngestorConfig(
        clickhouse=ClickHouseConfig(
            host="localhost",
            port=8123,
            database="zer0data",
            username="default",
            password="",
        ),
        data_dir="./data/test",
        batch_size=1000,
        max_workers=2,
    )


@pytest.fixture
def mock_kline_data():
    """Create mock kline data."""
    return [
        {
            "open_time": 1704067200000,
            "open": "50000.0",
            "high": "50100.0",
            "low": "49900.0",
            "close": "50050.0",
            "volume": "100.0",
            "close_time": 1704067259999,
            "quote_volume": "5000000.0",
            "trades": 500,
            "taker_buy_base": "60.0",
            "taker_buy_quote": "3000000.0",
        },
        {
            "open_time": 1704067260000,
            "open": "50050.0",
            "high": "50150.0",
            "low": "49950.0",
            "close": "50100.0",
            "volume": "110.0",
            "close_time": 1704067319999,
            "quote_volume": "5500000.0",
            "trades": 550,
            "taker_buy_base": "65.0",
            "taker_buy_quote": "3250000.0",
        },
    ]


def test_ingest_single_date(ingestor_config, mock_kline_data):
    """Test ingesting a single date with mocked downloader and writer."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader") as mock_downloader_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        # Setup mocks
        mock_downloader = MagicMock()
        mock_downloader.download_daily_klines.return_value = mock_kline_data
        mock_downloader_cls.return_value = mock_downloader

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        # Create ingestor and ingest
        ingestor = KlineIngestor(ingestor_config)
        target_date = date(2024, 1, 1)
        symbols = ["BTCUSDT", "ETHUSDT"]

        stats = ingestor.ingest_date(target_date, symbols)

        # Verify downloader was called for each symbol
        assert mock_downloader.download_daily_klines.call_count == 2
        mock_downloader.download_daily_klines.assert_any_call(
            symbol="BTCUSDT",
            date=target_date,
            interval="1m",
        )
        mock_downloader.download_daily_klines.assert_any_call(
            symbol="ETHUSDT",
            date=target_date,
            interval="1m",
        )

        # Verify writer insert was called for each kline record
        assert mock_writer.insert.call_count == 4  # 2 symbols * 2 klines
        assert mock_writer.flush.call_count == 1

        # Verify stats
        assert stats.symbols_processed == 2
        assert stats.dates_processed == 1
        assert stats.records_written == 4
        assert len(stats.errors) == 0

        # Cleanup
        ingestor.close()


def test_ingest_single_date_with_error(ingestor_config, mock_kline_data):
    """Test ingesting a single date with a download error."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader") as mock_downloader_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        # Setup mocks
        mock_downloader = MagicMock()
        mock_downloader.download_daily_klines.side_effect = [
            mock_kline_data,  # BTCUSDT succeeds
            Exception("Network error"),  # ETHUSDT fails
        ]
        mock_downloader_cls.return_value = mock_downloader

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        # Create ingestor and ingest
        ingestor = KlineIngestor(ingestor_config)
        target_date = date(2024, 1, 1)
        symbols = ["BTCUSDT", "ETHUSDT"]

        stats = ingestor.ingest_date(target_date, symbols)

        # Verify stats
        assert stats.symbols_processed == 1
        assert stats.records_written == 2
        assert len(stats.errors) == 1
        assert "ETHUSDT" in stats.errors[0]

        # Cleanup
        ingestor.close()


def test_ingest_date_range(ingestor_config, mock_kline_data):
    """Test backfilling a date range."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader") as mock_downloader_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        # Setup mocks
        mock_downloader = MagicMock()
        mock_downloader.download_daily_klines.return_value = mock_kline_data
        mock_downloader_cls.return_value = mock_downloader

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        # Create ingestor and backfill
        ingestor = KlineIngestor(ingestor_config)
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 2)
        symbols = ["BTCUSDT"]

        stats = ingestor.backfill(
            symbols=symbols,
            start=start_date,
            end=end_date,
            workers=2,
        )

        # Verify stats
        assert stats.symbols_processed == 2  # 2 dates * 1 symbol
        assert stats.dates_processed == 2
        assert stats.records_written == 4  # 2 dates * 2 klines
        assert len(stats.errors) == 0

        # Cleanup
        ingestor.close()


def test_ingestor_context_manager(ingestor_config):
    """Test using ingestor as a context manager."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader") as mock_downloader_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_downloader = MagicMock()
        mock_downloader.download_daily_klines.return_value = []
        mock_downloader_cls.return_value = mock_downloader

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        with KlineIngestor(ingestor_config) as ingestor:
            target_date = date(2024, 1, 1)
            ingestor.ingest_date(target_date, ["BTCUSDT"])

        # Verify close was called on both writer and downloader
        mock_writer.close.assert_called_once()
        mock_downloader.close.assert_called_once()


def test_get_date_range(ingestor_config):
    """Test the _get_date_range helper method."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"):

        ingestor = KlineIngestor(ingestor_config)

        # Test explicit dates
        start = date(2024, 1, 1)
        end = date(2024, 1, 3)
        date_range = ingestor._get_date_range(start, end)

        assert len(date_range) == 3
        assert date_range[0] == date(2024, 1, 1)
        assert date_range[-1] == date(2024, 1, 3)

        # Test default dates (None)
        date_range = ingestor._get_date_range(None, None)

        assert len(date_range) == 30  # Default 30 days


def test_ingestor_close_after_closed(ingestor_config):
    """Test that closing an already closed ingestor is idempotent."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader") as mock_downloader_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_downloader = MagicMock()
        mock_downloader_cls.return_value = mock_downloader

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()

        # Close again - should not call close on writer/downloader again
        ingestor.close()

        assert mock_writer.close.call_count == 1
        assert mock_downloader.close.call_count == 1


def test_ingest_after_close_raises_error(ingestor_config):
    """Test that using ingestor after close raises an error."""
    with patch("zer0data_ingestor.ingestor.BinanceKlineDownloader"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"):

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()

        with pytest.raises(RuntimeError, match="Ingestor has been closed"):
            ingestor.ingest_date(date(2024, 1, 1), ["BTCUSDT"])
