"""ClickHouse writer for kline data — DataFrame edition."""

import logging
from typing import Set

import clickhouse_connect
import pandas as pd

from zer0data_ingestor.constants import VALID_INTERVALS, is_valid_interval
from zer0data_ingestor.schema import CLICKHOUSE_COLUMN_TYPES, KLINE_COLUMNS

logger = logging.getLogger(__name__)


class ClickHouseWriter:
    """Writer for streaming kline DataFrames to ClickHouse."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "zer0data",
        table: str = "klines",
        username: str = "default",
        password: str = "",
    ):
        """Initialize ClickHouse writer.

        Args:
            host: ClickHouse server host.
            port: ClickHouse HTTP port.
            database: Database name.
            table: Target table prefix (actual tables: ``{table}_{interval}``).
            username: Database username.
            password: Database password.
        """
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self.table = table

        # Ensure all interval tables exist at startup.
        self._init_tables()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_df(self, df: pd.DataFrame, interval: str) -> None:
        """Write a DataFrame directly to the appropriate interval table.

        Args:
            df: DataFrame whose columns match ``schema.KLINE_COLUMNS``.
            interval: The k-line interval (e.g. ``"1m"``, ``"1h"``).

        Raises:
            ValueError: If the interval is not valid.
        """
        if df.empty:
            return

        if not is_valid_interval(interval):
            raise ValueError(
                f"Invalid interval '{interval}' — cannot determine target table."
            )

        table = self._get_table_name(interval)
        # Ensure the DataFrame column order matches the table schema.
        df_ordered = df[KLINE_COLUMNS]
        self.client.insert_df(table, df_ordered)

    def has_data_for_date(
        self, symbol: str, interval: str, date_str: str
    ) -> bool:
        """Check if data exists for a specific symbol, interval, and date.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT").
            interval: The k-line interval (e.g., "1m", "1h").
            date_str: Date string in format "YYYY-MM-DD".

        Returns:
            True if any data exists for the given date, False otherwise.
        """
        if not is_valid_interval(interval):
            return False

        table = self._get_table_name(interval)

        # Convert date string to timestamp range (milliseconds)
        date = pd.to_datetime(date_str)
        start_ts = int(date.timestamp() * 1000)
        end_ts = int((date + pd.Timedelta(days=1)).timestamp() * 1000)

        query = f"""
            SELECT count() as cnt
            FROM {table}
            WHERE symbol = %(symbol)s
              AND open_time >= %(start_ts)s
              AND open_time < %(end_ts)s
        """

        result = self.client.query(
            query,
            parameters={
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

        if result.result_rows:
            return result.result_rows[0][0] > 0
        return False

    def has_data_for_month(
        self, symbol: str, interval: str, year: int, month: int
    ) -> bool:
        """Check if data exists for a specific symbol, interval, and month.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT").
            interval: The k-line interval (e.g., "1m", "1h").
            year: Year (e.g., 2025).
            month: Month (1-12).

        Returns:
            True if any data exists for the given month, False otherwise.
        """
        if not is_valid_interval(interval):
            return False

        table = self._get_table_name(interval)

        # Convert to timestamp range for the entire month
        start_date = pd.Timestamp(year=year, month=month, day=1)
        if month == 12:
            end_date = pd.Timestamp(year=year + 1, month=1, day=1)
        else:
            end_date = pd.Timestamp(year=year, month=month + 1, day=1)

        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)

        query = f"""
            SELECT count() as cnt
            FROM {table}
            WHERE symbol = %(symbol)s
              AND open_time >= %(start_ts)s
              AND open_time < %(end_ts)s
        """

        result = self.client.query(
            query,
            parameters={
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

        if result.result_rows:
            return result.result_rows[0][0] > 0
        return False

    def close(self) -> None:
        """Close the underlying ClickHouse client."""
        self.client.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_tables(self) -> None:
        """Ensure all interval tables exist (called once at startup).

        Iterates over every valid interval and creates missing tables.
        This keeps DDL out of the write path and surfaces connection
        problems early.
        """
        for interval in VALID_INTERVALS:
            table = self._get_table_name(interval)
            if not self._table_exists(table):
                logger.info("Creating table %s", table)
                self._create_table(table)

    def _get_table_name(self, interval: str) -> str:
        """Get the table name for a given interval."""
        return f"{self.table}_{interval}"

    def _table_exists(self, table: str) -> bool:
        """Check if a table exists in the database."""
        result = self.client.query(f"EXISTS TABLE {table}")
        return len(result.result_rows) > 0 and result.result_rows[0][0] == 1

    def _create_table(self, table: str) -> None:
        """Create a klines table with the specified name."""
        col_defs = ",\n                ".join(
            f"{col} {CLICKHOUSE_COLUMN_TYPES[col]}" for col in KLINE_COLUMNS
        )
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {col_defs}
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (symbol, open_time)
            PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
        """
        self.client.command(create_sql)
