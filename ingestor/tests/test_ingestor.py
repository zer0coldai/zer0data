"""Tests for KlineIngestor — DataFrame edition."""

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.cleaner.kline import CleanResult, CleaningStats
from zer0data_ingestor.ingestor import KlineIngestor, IngestStats


# ---------------------------------------------------------------------------
# Fixtures / Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def ingestor_config():
    return IngestorConfig(
        clickhouse=ClickHouseConfig(
            host="localhost",
            port=8123,
            database="zer0data",
            username="default",
            password="",
        ),
    )


def _sample_df(symbol="BTCUSDT", interval="1m", n=1, start_time=1704067200000):
    rows = []
    for i in range(n):
        rows.append({
            "symbol": symbol,
            "open_time": start_time + i * 60000,
            "close_time": start_time + 59999 + i * 60000,
            "open_price": 50000.0 + i,
            "high_price": 50100.0 + i,
            "low_price": 49900.0 + i,
            "close_price": 50050.0 + i,
            "volume": 100.0 + i,
            "quote_volume": 5000000.0 + i * 1000,
            "trades_count": 1000 + i,
            "taker_buy_volume": 60.0 + i,
            "taker_buy_quote_volume": 3000000.0 + i * 1000,
            "interval": interval,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_ingest_from_directory(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        df_btc = _sample_df("BTCUSDT")
        df_eth = _sample_df("ETHUSDT")
        mock_parser.parse_directory.return_value = [
            ("BTCUSDT", "1m", df_btc),
            ("ETHUSDT", "1m", df_eth),
        ]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT", "ETHUSDT"], "*.zip")

        mock_parser.parse_directory.assert_called_once_with(
            "/data/klines", ["BTCUSDT", "ETHUSDT"], pattern="*.zip",
        )

        # Writer should be called for each symbol.
        assert mock_writer.write_df.call_count == 2
        assert stats.files_processed == 2
        assert stats.records_written == 2
        assert stats.symbols_processed == 2
        assert len(stats.errors) == 0

        ingestor.close()


def test_ingest_from_directory_with_error(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        mock_parser.parse_directory.side_effect = Exception("Parse error")
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        assert len(stats.errors) == 1
        assert "Parse error" in stats.errors[0]
        assert stats.records_written == 0

        ingestor.close()


def test_ingest_after_close_raises(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"):

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()

        with pytest.raises(RuntimeError, match="Ingestor has been closed"):
            ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])


def test_context_manager(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = []
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        with KlineIngestor(ingestor_config) as ingestor:
            ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        mock_writer.close.assert_called_once()


def test_close_is_idempotent(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser"), \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        ingestor.close()
        ingestor.close()

        assert mock_writer.close.call_count == 1


def test_default_pattern_is_recursive(ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = []
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        ingestor.ingest_from_directory("/data", ["BTCUSDT"])

        mock_parser.parse_directory.assert_called_once_with(
            "/data", ["BTCUSDT"], pattern="**/*.zip",
        )

        ingestor.close()


def test_cleaner_integration(ingestor_config):
    """Ingestor should clean data (dedup) before writing."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        # DataFrame with a duplicate row.
        df = _sample_df("BTCUSDT", n=2, start_time=1000)
        dup_row = df.iloc[[0]].copy()
        df = pd.concat([df, dup_row], ignore_index=True)

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [("BTCUSDT", "1m", df)]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        assert stats.duplicates_removed == 1
        # Only 2 unique rows should be written.
        assert stats.records_written == 2

        ingestor.close()


def test_logs_cleaning_stats(caplog, ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        df = _sample_df("BTCUSDT", n=1, start_time=1000)
        dup = df.copy()
        df = pd.concat([df, dup], ignore_index=True)

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [("BTCUSDT", "1m", df)]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        with caplog.at_level(logging.INFO):
            stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        assert stats.duplicates_removed == 1

        symbol_logs = [r for r in caplog.records if "Symbol BTCUSDT" in r.message]
        assert len(symbol_logs) == 1
        assert "removed 1 duplicates" in symbol_logs[0].message

        overall_logs = [r for r in caplog.records if "Ingestion complete" in r.message]
        assert len(overall_logs) == 1

        ingestor.close()


def test_no_cleaning_log_when_all_zero(caplog, ingestor_config):
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:

        df = _sample_df("BTCUSDT")

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [("BTCUSDT", "1m", df)]
        mock_parser_cls.return_value = mock_parser

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        ingestor = KlineIngestor(ingestor_config)
        with caplog.at_level(logging.INFO):
            stats = ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        assert stats.duplicates_removed == 0
        assert stats.gaps_filled == 0
        assert stats.invalid_records_removed == 0
        assert stats.records_written == 1

        symbol_logs = [r for r in caplog.records if "Symbol BTCUSDT" in r.message]
        assert len(symbol_logs) == 0

        overall_logs = [r for r in caplog.records if "Ingestion complete" in r.message]
        assert len(overall_logs) == 1

        ingestor.close()


def test_uses_interval_to_ms(ingestor_config):
    """Ingestor should derive cleaner interval_ms from the record interval."""
    with patch("zer0data_ingestor.ingestor.KlineParser") as mock_parser_cls, \
         patch("zer0data_ingestor.ingestor.ClickHouseWriter"), \
         patch("zer0data_ingestor.ingestor.KlineCleaner") as mock_cleaner_cls:

        df = _sample_df("BTCUSDT", interval="1h")

        mock_parser = MagicMock()
        mock_parser.parse_directory.return_value = [("BTCUSDT", "1h", df)]
        mock_parser_cls.return_value = mock_parser

        mock_cleaner = MagicMock()
        mock_cleaner.clean.return_value = CleanResult(
            cleaned_df=pd.DataFrame(), stats=CleaningStats()
        )
        mock_cleaner_cls.return_value = mock_cleaner

        ingestor = KlineIngestor(ingestor_config)
        ingestor.ingest_from_directory("/data/klines", ["BTCUSDT"])

        mock_cleaner_cls.assert_called_once_with(interval_ms=3_600_000)
