"""ClickHouse writer for kline data."""

from dataclasses import dataclass
from typing import List
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
            table: Target table name
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
        self._buffer: List[KlineRecord] = []
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._batch_size = batch_size

    def insert(self, record: KlineRecord) -> None:
        """Insert a single kline record.

        Args:
            record: KlineRecord to insert
        """
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def insert_many(self, records: List[KlineRecord]) -> None:
        """Insert multiple kline records.

        Args:
            records: KlineRecord list to append into write buffer
        """
        if not records:
            return

        self._buffer.extend(records)
        while len(self._buffer) >= self._batch_size:
            batch = self._buffer[:self._batch_size]
            self._write_batch(batch)
            del self._buffer[:self._batch_size]

    def flush(self) -> None:
        """Flush buffered records to ClickHouse."""
        if not self._buffer:
            return
        self._write_batch(self._buffer)
        self._buffer.clear()

    def _write_batch(self, records: List[KlineRecord]) -> None:
        """Write one batch to ClickHouse."""
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
            ]
            for r in records
        ]

        self.client.insert(
            self.table,
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
            ],
        )

    def close(self) -> None:
        """Close the writer and flush remaining records."""
        self.flush()
        self.client.close()
