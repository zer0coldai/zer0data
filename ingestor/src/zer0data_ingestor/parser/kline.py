"""Kline CSV parser for Binance data."""

from datetime import datetime
from typing import Iterator
import csv

from zer0data_ingestor.writer.clickhouse import KlineRecord


def _ms_to_datetime(ms: int) -> datetime:
    """Convert milliseconds timestamp to datetime.

    Args:
        ms: Milliseconds since Unix epoch

    Returns:
        datetime object
    """
    return datetime.fromtimestamp(ms / 1000.0)


def parse_klines_csv(file_path: str, symbol: str) -> Iterator[KlineRecord]:
    """Parse Binance klines CSV file and yield KlineRecord objects.

    Binance CSV format has 12 columns:
    0: Open time
    1: Open price
    2: High price
    3: Low price
    4: Close price
    5: Volume
    6: Close time
    7: Quote asset volume
    8: Number of trades
    9: Taker buy base asset volume
    10: Taker buy quote asset volume
    11: Ignore

    Args:
        file_path: Path to the CSV file
        symbol: Trading symbol (e.g., "BTCUSDT")

    Yields:
        KlineRecord objects

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If CSV format is invalid
    """
    with open(file_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 12:
                continue

            try:
                yield KlineRecord(
                    symbol=symbol,
                    open_time=int(row[0]),
                    close_time=int(row[6]),
                    open_price=float(row[1]),
                    high_price=float(row[2]),
                    low_price=float(row[3]),
                    close_price=float(row[4]),
                    volume=float(row[5]),
                    quote_volume=float(row[7]),
                    trades_count=int(row[8]),
                    taker_buy_volume=float(row[9]),
                    taker_buy_quote_volume=float(row[10]),
                )
            except (ValueError, IndexError) as e:
                raise ValueError(f"Invalid CSV format in row {row}: {e}") from e
