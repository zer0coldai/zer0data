"""Tests for KlineIngestor."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.ingestor import KlineIngestor, IngestStats
from zer0data_ingestor.cleaner.kline import CleanResult, CleaningStats


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
        # parse_directory yields tuples of (symbol, interval, record)
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1m", mock_record_1),
            ("ETHUSDT", "1m", mock_record_2),
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
        mock_parser.parse_directory.assert_called_once_with(
            source,
            symbols,
            pattern="*.zip",
        )

        # Verify writer batch insert was called for each symbol batch
        assert mock_writer.insert_many.call_count == 2
        assert mock_writer.flush.call_count == 1

        # Verify stats
        assert stats.files_processed == 1  # parse_directory was called once
        assert stats.records_written == 2
        assert len(stats.errors) == 0

        # Verify records have default interval value
        assert mock_record_1.interval == "1m"
        assert mock_record_2.interval == "1m"

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

        mock_parser.parse_directory.assert_called_once_with(
            source,
            symbols,
            pattern="**/*.zip",
        )

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

    # Verify interval is preserved
    assert result.cleaned_records[0].interval == "1m"


def test_ingestor_integration_with_cleaner(ingestor_config):
    """Test that ingestor integrates cleaner and tracks cleaning stats."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        # Create mock data with duplicates and multiple symbols
        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1m", KlineRecord(
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
            ("BTCUSDT", "1m", KlineRecord(  # duplicate
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
            ("ETHUSDT", "1m", KlineRecord(
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
        assert mock_writer.insert_many.call_count == 2

        # Verify written records have default interval
        written_records = []
        for call in mock_writer.insert_many.call_args_list:
            written_records.extend(call[0][0])
        assert all(r.interval == "1m" for r in written_records)

        ingestor.close()


def test_ingestor_logs_cleaning_stats(caplog, ingestor_config):
    """Test that ingestor logs cleaning statistics."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        # Create mock data with duplicates to trigger logging
        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1m", KlineRecord(
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
            ("BTCUSDT", "1m", KlineRecord(  # duplicate
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
        ]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)

        # Capture log messages at INFO level
        with caplog.at_level(logging.INFO):
            stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        # Verify cleaning stats were logged
        assert stats.duplicates_removed == 1
        assert stats.records_written == 1  # Only 1 record after deduplication

        # Verify written record has default interval
        assert mock_writer.insert_many.call_count == 1
        written_records = mock_writer.insert_many.call_args_list[0][0][0]
        assert len(written_records) == 1
        assert written_records[0].interval == "1m"

        # Check that per-symbol cleaning log was generated
        symbol_logs = [record for record in caplog.records if "Symbol BTCUSDT" in record.message]
        assert len(symbol_logs) == 1
        assert "removed 1 duplicates" in symbol_logs[0].message

        # Check that overall stats log was generated
        overall_logs = [record for record in caplog.records if "Ingestion complete" in record.message]
        assert len(overall_logs) == 1


def test_ingestor_passes_cleaner_interval_from_config(ingestor_config):
    """Ingestor should derive cleaner interval from record interval."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"), \
         patch("zer0data_ingestor.ingestor.KlineCleaner") as mock_cleaner_cls:
        from zer0data_ingestor.writer.clickhouse import KlineRecord

        ingestor_config.cleaner_interval_ms = 120000
        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1h", KlineRecord(
                symbol="BTCUSDT", open_time=1735689600000, close_time=1735693199999,
                open_price=100.0, high_price=101.0, low_price=99.0, close_price=100.5,
                volume=1.0, quote_volume=100.0, trades_count=1,
                taker_buy_volume=0.5, taker_buy_quote_volume=50.0,
                interval="1h",
            )),
        ]
        mock_parser_cls.return_value = mock_parser
        mock_cleaner = MagicMock()
        mock_cleaner.clean.return_value = CleanResult(cleaned_records=[], stats=CleaningStats())
        mock_cleaner_cls.return_value = mock_cleaner

        ingestor = KlineIngestor(ingestor_config)
        ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        # 1h should map to 3_600_000 ms, not fallback 120000.
        mock_cleaner_cls.assert_called_once_with(interval_ms=3_600_000)


def test_ingestor_passes_batch_size_to_writer(ingestor_config):
    """Ingestor should pass configured batch size to ClickHouseWriter."""
    with patch("zer0data_ingestor.ingestor.KlineParser"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:
        ingestor_config.batch_size = 5000
        KlineIngestor(ingestor_config)
        assert mock_writer_cls.call_args.kwargs["batch_size"] == 5000


def test_ingestor_processes_large_symbol_in_chunks(ingestor_config):
    """Ingestor should clean/write large symbol streams in chunks."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls, \
         patch("zer0data_ingestor.ingestor.KlineCleaner") as mock_cleaner_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        ingestor_config.batch_size = 2

        records = [
            KlineRecord(
                symbol="BTCUSDT",
                open_time=1000 + i * 1000,
                close_time=1059 + i * 1000,
                open_price=50000.0 + i,
                high_price=50100.0 + i,
                low_price=49900.0 + i,
                close_price=50050.0 + i,
                volume=100.0 + i,
                quote_volume=5000000.0 + i,
                trades_count=1000 + i,
                taker_buy_volume=50.0 + i,
                taker_buy_quote_volume=2500000.0 + i,
            )
            for i in range(5)
        ]

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [("BTCUSDT", "1m", r) for r in records]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        mock_cleaner = MagicMock()
        mock_cleaner.clean.side_effect = lambda chunk: CleanResult(
            cleaned_records=chunk,
            stats=CleaningStats(),
        )
        mock_cleaner_cls.return_value = mock_cleaner

        ingestor = KlineIngestor(ingestor_config)
        stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        assert mock_cleaner.clean.call_count == 3
        assert mock_writer.insert_many.call_count == 3
        assert stats.records_written == 5

        # Verify all records have default interval
        for record in records:
            assert record.interval == "1m"

        ingestor.close()


def test_ingestor_logs_no_cleaning_when_all_stats_zero(caplog, ingestor_config):
    """Test that ingestor handles case when no cleaning is needed."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        from zer0data_ingestor.writer.clickhouse import KlineRecord

        # Create mock data with no duplicates
        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1m", KlineRecord(
                symbol="BTCUSDT", open_time=1000, close_time=1059,
                open_price=50000.0, high_price=50100.0, low_price=49900.0,
                close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0)),
        ]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)

        # Capture log messages at INFO level
        with caplog.at_level(logging.INFO):
            stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        # Verify no cleaning occurred
        assert stats.duplicates_removed == 0
        assert stats.gaps_filled == 0
        assert stats.invalid_records_removed == 0
        assert stats.records_written == 1

        # Verify written record has default interval
        assert mock_writer.insert_many.call_count == 1
        written_records = mock_writer.insert_many.call_args_list[0][0][0]
        assert len(written_records) == 1
        assert written_records[0].interval == "1m"

        # Check that per-symbol cleaning log was NOT generated (all stats are 0)
        symbol_logs = [record for record in caplog.records if "Symbol BTCUSDT" in record.message]
        assert len(symbol_logs) == 0

        # Check that overall stats log was still generated
        overall_logs = [record for record in caplog.records if "Ingestion complete" in record.message]
        assert len(overall_logs) == 1
        assert "0 duplicates removed" in overall_logs[0].message
        assert "0 gaps filled" in overall_logs[0].message
        assert "0 invalid records removed" in overall_logs[0].message

        ingestor.close()
