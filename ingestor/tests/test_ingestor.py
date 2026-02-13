"""Tests for KlineIngestor."""

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


def test_ingest_from_directory(ingestor_config):
    """Test ingesting kline data from a directory with mocked parser and writer."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        # Setup mock parser to yield some test records
        from zer0data_ingestor.writer.clickhouse import KlineRecord

        mock_parser = MagicMock()
        mock_record_1 = KlineRecord(
            symbol="BTCUSDT",
            open_time=1704067200000,
            close_time=1704067259999,
            open_price=50000.0,
            high_price=50100.0,
            low_price=49900.0,
            close_price=50050.0,
            volume=100.0,
            quote_volume=5000000.0,
            trades_count=500,
            taker_buy_volume=60.0,
            taker_buy_quote_volume=3000000.0,
        )
        mock_record_2 = KlineRecord(
            symbol="ETHUSDT",
            open_time=1704067200000,
            close_time=1704067259999,
            open_price=3000.0,
            high_price=3010.0,
            low_price=2990.0,
            close_price=3005.0,
            volume=200.0,
            quote_volume=600000.0,
            trades_count=300,
            taker_buy_volume=120.0,
            taker_buy_quote_volume=360000.0,
        )
        # parse_directory yields tuples of (symbol, record)
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", mock_record_1),
            ("ETHUSDT", mock_record_2),
        ]
        mock_parser_cls.return_value = mock_parser

        # Setup mock writer
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        # Create ingestor and ingest from directory
        ingestor = KlineIngestor(ingestor_config)
        source = "/data/klines"
        symbols = ["BTCUSDT", "ETHUSDT"]

        stats = ingestor.ingest_from_directory(source, symbols, "*.zip")

        # Verify parser was called correctly
        mock_parser.parse_directory.assert_called_once_with(source, symbols, "*.zip")

        # Verify writer insert was called for each record
        assert mock_writer.insert.call_count == 2
        assert mock_writer.flush.call_count == 1

        # Verify stats
        assert stats.files_processed == 1  # parse_directory was called once
        assert stats.records_written == 2
        assert len(stats.errors) == 0

        # Cleanup
        ingestor.close()


def test_ingest_from_directory_with_error(ingestor_config):
    """Test ingesting from directory with a parsing error."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        mock_parser = MagicMock()
        # Make parse_directory raise an error
        mock_parser.parse_directory.side_effect = Exception("Parse error")
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)

        stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        # Verify error was captured
        assert len(stats.errors) == 1
        assert "Parse error" in stats.errors[0]
        assert stats.records_written == 0

        ingestor.close()


def test_ingest_after_close_raises_error(ingestor_config):
    """Test that using ingestor after close raises an error."""
    with patch("zer0data_ingestor.ingestor.KlineParser"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"):

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()

        with pytest.raises(RuntimeError, match="Ingestor has been closed"):
            ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])


def test_ingestor_context_manager(ingestor_config):
    """Test using ingestor as a context manager."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = []
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        with KlineIngestor(ingestor_config) as ingestor:
            ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        # Verify close was called on writer
        mock_writer.close.assert_called_once()


def test_ingestor_close_after_closed(ingestor_config):
    """Test that closing an already closed ingestor is idempotent."""
    with patch("zer0data_ingestor.ingestor.KlineParser"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()

        # Close again - should not call close on writer again
        ingestor.close()

        assert mock_writer.close.call_count == 1


def test_ingest_from_directory_uses_recursive_default_pattern(ingestor_config):
    """Default pattern should recurse into nested download directories."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = []
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        source = "/data"
        symbols = ["BTCUSDT"]

        ingestor.ingest_from_directory(source, symbols)

        mock_parser.parse_directory.assert_called_once_with(source, symbols, "**/*.zip")

        ingestor.close()


def test_ingestor_cleans_data_before_writing(ingestor_config):
    """Test that ingestor applies cleaning before writing to database."""
    from unittest.mock import Mock
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    # Create mock data with duplicates
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,  # duplicate
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    ]

    # Manually test cleaning
    from zer0data_ingestor.cleaner.kline import KlineCleaner
    cleaner = KlineCleaner()
    result = cleaner.clean(records)

    # Should only have 1 record after cleaning
    assert len(result.cleaned_records) == 1
    assert result.stats.duplicates_removed == 1


def test_ingestor_integration_with_cleaner(ingestor_config):
    """Test that ingestor integrates cleaner and tracks cleaning stats."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        # Create mock data with duplicates and multiple symbols
        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", KlineRecord(
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
            ("BTCUSDT", KlineRecord(  # duplicate
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
            ("ETHUSDT", KlineRecord(
                symbol="ETHUSDT", open_time=2000, close_time=2059,
                open_price=3000.0, high_price=3010.0, low_price=2990.0,
                close_price=3005.0, volume=200.0, quote_volume=600000.0,
                trades_count=500, taker_buy_volume=100.0, taker_buy_quote_volume=300000.0)),
        ]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        # Create ingestor and ingest
        ingestor = KlineIngestor(ingestor_config)
        source = "/data/klines"
        symbols = ["BTCUSDT", "ETHUSDT"]

        stats = ingestor.ingest_from_directory(source, symbols)

        # Verify stats include cleaning information
        assert hasattr(stats, "duplicates_removed")
        assert hasattr(stats, "gaps_filled")
        assert hasattr(stats, "invalid_records_removed")

        # Should have removed 1 duplicate
        assert stats.duplicates_removed == 1

        # Writer should be called with cleaned data (2 records: 1 BTCUSDT + 1 ETHUSDT)
        assert mock_writer.insert.call_count == 2

        ingestor.close()
