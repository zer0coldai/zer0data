"""ClickHouse writer for kline data."""

from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict
import clickhouse_connect


@dataclass
class KlineRecord:
    """Kline (candlestick) data record."""

    symbol: str
    open_time: int
    close_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trades_count: int
    taker_buy_volume: float
    taker_buy_quote_volume: float
    interval: str = "1m"


class ClickHouseWriter:
    """Writer for streaming kline data to ClickHouse."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "zer0data",
        table: str = "klines",
        username: str = "default",
        password: str = "",
        batch_size: int = 1000,
    ):
        """Initialize ClickHouse writer.

        Args:
            host: ClickHouse server host
            port: ClickHouse HTTP port
            database: Database name
            table: Target table prefix (default "klines")
            username: Database username
            password: Database password
            batch_size: Number of records buffered before each insert
        """
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self.table = table
        self._buffer: Dict[str, List[KlineRecord]] = defaultdict(list)
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._batch_size = batch_size

    def _get_table_name(self, interval: str) -> str:
        """Get the table name for a given interval.

        Args:
            interval: The k-line interval (e.g., "1m", "1h", "1d")

        Returns:
            The table name in format "klines_{interval}"
        """
        return f"{self.table}_{interval}"

    def _table_exists(self, table: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table: The table name to check

        Returns:
            True if the table exists, False otherwise
        """
        result = self.client.query(f"EXISTS TABLE {table}")
        return len(result.result_rows) > 0 and result.result_rows[0][0] == 1

    def _create_table(self, table: str) -> None:
        """Create a klines table with the specified name.

        Args:
            table: The table name to create
        """
        create_sql = f"""
            CREATE TABLE {table} (
                symbol String,
                open_time Int64,
                close_time Int64,
                open_price Float64,
                high_price Float64,
                low_price Float64,
                close_price Float64,
                volume Float64,
                quote_volume Float64,
                trades_count Int64,
                taker_buy_volume Float64,
                taker_buy_quote_volume Float64,
                interval String
            ) ENGINE = MergeTree()
            ORDER BY (symbol, open_time)
        """
        self.client.command(create_sql)

    def _ensure_table_exists(self, table: str) -> None:
        """Ensure a table exists, creating it if necessary.

        Args:
            table: The table name to ensure exists
        """
        if not self._table_exists(table):
            self._create_table(table)

    def insert(self, record: KlineRecord) -> None:
        """Insert a single kline record.

        Args:
            record: KlineRecord to insert
        """
        self._buffer[record.interval].append(record)
        if len(self._buffer[record.interval]) >= self._batch_size:
            self._flush_interval(record.interval)

    def insert_many(self, records: List[KlineRecord]) -> None:
        """Insert multiple kline records.

        Args:
            records: KlineRecord list to append into write buffer
        """
        if not records:
            return

        # Group records by interval in buffer
        for record in records:
            self._buffer[record.interval].append(record)

        # Check if any interval has reached batch size
        for interval, interval_records in list(self._buffer.items()):
            while len(interval_records) >= self._batch_size:
                batch = interval_records[:self._batch_size]
                self._write_batch(interval, batch)
                del interval_records[:self._batch_size]

    def flush(self) -> None:
        """Flush buffered records to ClickHouse."""
        if not self._buffer:
            return
        # Flush all interval buffers
        for interval, records in list(self._buffer.items()):
            if records:
                self._write_batch(interval, records)
                records.clear()

    def _flush_interval(self, interval: str) -> None:
        """Flush buffered records for a specific interval.

        Args:
            interval: The interval to flush
        """
        if interval in self._buffer and self._buffer[interval]:
            self._write_batch(interval, self._buffer[interval])
            self._buffer[interval].clear()

    def _write_batch(self, interval: str, records: List[KlineRecord]) -> None:
        """Write one batch to ClickHouse.

        Args:
            interval: The k-line interval for these records
            records: List of KlineRecord to write
        """
        table = self._get_table_name(interval)
        self._ensure_table_exists(table)
        data = [
            [
                r.symbol,
                r.open_time,
                r.close_time,
                r.open_price,
                r.high_price,
                r.low_price,
                r.close_price,
                r.volume,
                r.quote_volume,
                r.trades_count,
                r.taker_buy_volume,
                r.taker_buy_quote_volume,
                r.interval,
            ]
            for r in records
        ]

        self.client.insert(
            table,
            data,
            column_names=[
                "symbol",
                "open_time",
                "close_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "interval",
            ],
        )

    def close(self) -> None:
        """Close the writer and flush remaining records."""
        self.flush()
        self.client.close()
