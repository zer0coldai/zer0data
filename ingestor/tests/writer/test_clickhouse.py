"""Tests for ClickHouseWriter — DataFrame edition."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

from zer0data_ingestor.constants import VALID_INTERVALS
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter
from zer0data_ingestor.schema import KLINE_COLUMNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_client_all_tables_exist():
    """Return a mock client where all tables already exist."""
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = [(1,)]
    return mock_client


def _mock_client_no_tables():
    """Return a mock client where no tables exist."""
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = [(0,)]
    return mock_client


def _sample_df(n: int = 1, interval: str = "1m", symbol: str = "BTCUSDT") -> pd.DataFrame:
    """Create a small kline DataFrame with *n* rows."""
    rows = []
    for i in range(n):
        rows.append({
            "symbol": symbol,
            "open_time": 1700000000000 + i * 60000,
            "close_time": 1700000059999 + i * 60000,
            "open_price": 50000.0 + i,
            "high_price": 51000.0 + i,
            "low_price": 49000.0 + i,
            "close_price": 50500.0 + i,
            "volume": 100.0 + i,
            "quote_volume": 5000000.0 + i * 1000,
            "trades_count": 1000 + i,
            "taker_buy_volume": 60.0 + i,
            "taker_buy_quote_volume": 3000000.0 + i * 1000,
            "interval": interval,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _init_tables (startup)
# ---------------------------------------------------------------------------

class TestInitTables:
    """Startup table initialisation."""

    def test_creates_missing_tables(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_no_tables()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()

            # Should have checked + created a table for every valid interval.
            assert mock_client.query.call_count == len(VALID_INTERVALS)
            assert mock_client.command.call_count == len(VALID_INTERVALS)

    def test_skips_existing_tables(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()

            # Checked every interval, but created none.
            assert mock_client.query.call_count == len(VALID_INTERVALS)
            mock_client.command.assert_not_called()


# ---------------------------------------------------------------------------
# write_df
# ---------------------------------------------------------------------------

class TestWriteDf:
    """Tests for write_df() method."""

    def test_write_calls_insert_df(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            df = _sample_df(3, interval="1m")
            writer.write_df(df, "1m")

            mock_client.insert_df.assert_called_once()
            args = mock_client.insert_df.call_args
            assert args[0][0] == "klines_1m"
            written_df = args[0][1]
            assert len(written_df) == 3
            assert list(written_df.columns) == KLINE_COLUMNS

    def test_write_empty_df_is_noop(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            writer.write_df(pd.DataFrame(), "1m")

            mock_client.insert_df.assert_not_called()

    def test_write_invalid_interval_raises(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            with pytest.raises(ValueError, match="Invalid interval"):
                writer.write_df(_sample_df(), "invalid")

    def test_write_routes_to_correct_table(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            writer.write_df(_sample_df(interval="1h"), "1h")

            assert mock_client.insert_df.call_args[0][0] == "klines_1h"

    def test_write_multiple_intervals(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            writer.write_df(_sample_df(3, interval="1m"), "1m")
            writer.write_df(_sample_df(2, interval="1h", symbol="ETHUSDT"), "1h")

            assert mock_client.insert_df.call_count == 2
            tables = [c[0][0] for c in mock_client.insert_df.call_args_list]
            assert "klines_1m" in tables
            assert "klines_1h" in tables


# ---------------------------------------------------------------------------
# Table name helpers
# ---------------------------------------------------------------------------

class TestTableName:
    def test_get_table_name(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_get.return_value = _mock_client_all_tables_exist()

            writer = ClickHouseWriter()
            assert writer._get_table_name("1m") == "klines_1m"
            assert writer._get_table_name("1h") == "klines_1h"
            assert writer._get_table_name("1d") == "klines_1d"


# ---------------------------------------------------------------------------
# _create_table schema
# ---------------------------------------------------------------------------

class TestCreateTable:
    def test_creates_with_correct_schema(self):
        with patch("zer0data_ingestor.writer.clickhouse.clickhouse_connect.get_client") as mock_get:
            mock_client = _mock_client_all_tables_exist()
            mock_get.return_value = mock_client

            writer = ClickHouseWriter()
            # Reset command calls from _init_tables (all existed, so no commands).
            mock_client.command.reset_mock()

            writer._create_table("klines_5m")

            mock_client.command.assert_called_once()
            sql = mock_client.command.call_args[0][0]

            assert "CREATE TABLE IF NOT EXISTS" in sql
            assert "klines_5m" in sql
            assert "symbol String" in sql
            assert "open_time Int64" in sql
            assert "close_time Int64" in sql
            assert "open_price Float64" in sql
            assert "volume Float64" in sql
            assert "trades_count Int64" in sql
            assert "interval String" in sql
            assert "ENGINE = MergeTree()" in sql
            assert "ORDER BY" in sql
            assert "PARTITION BY" in sql
