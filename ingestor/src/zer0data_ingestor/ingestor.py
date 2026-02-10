"""Main ingestion logic for kline data."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.downloader import BinanceKlineDownloader
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord


@dataclass
class IngestStats:
    """Statistics for ingestion operations."""

    symbols_processed: int = 0
    dates_processed: int = 0
    records_written: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class KlineIngestor:
    """Main ingestor for downloading and writing kline data."""

    def __init__(self, config: IngestorConfig):
        """Initialize the ingestor.

        Args:
            config: IngestorConfig instance with database and download settings
        """
        self.config = config
        self.downloader = BinanceKlineDownloader()
        self.writer = ClickHouseWriter(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            username=config.clickhouse.username or "default",
            password=config.clickhouse.password or "",
        )
        self._closed = False

    def ingest_date(
        self, target_date: date, symbols: Optional[List[str]] = None
    ) -> IngestStats:
        """Ingest kline data for a single date.

        Args:
            target_date: Date to ingest data for
            symbols: List of symbols to ingest. If None, uses default top symbols.

        Returns:
            IngestStats with ingestion statistics
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        stats = IngestStats()

        # Use default symbols if none provided
        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]

        for symbol in symbols:
            try:
                # Download data for this symbol and date
                klines = self.downloader.download_daily_klines(
                    symbol=symbol,
                    date=target_date,
                    interval="1m",
                )

                # Write to ClickHouse
                for kline in klines:
                    record = KlineRecord(
                        symbol=symbol,
                        open_time=kline["open_time"],
                        close_time=kline["close_time"],
                        open_price=float(kline["open"]),
                        high_price=float(kline["high"]),
                        low_price=float(kline["low"]),
                        close_price=float(kline["close"]),
                        volume=float(kline["volume"]),
                        quote_volume=float(kline["quote_volume"]),
                        trades_count=kline["trades"],
                        taker_buy_volume=float(kline["taker_buy_base"]),
                        taker_buy_quote_volume=float(kline["taker_buy_quote"]),
                    )
                    self.writer.insert(record)
                    stats.records_written += 1

                stats.symbols_processed += 1

            except Exception as e:
                error_msg = f"Error processing {symbol} on {target_date}: {e}"
                stats.errors.append(error_msg)

        # Flush any remaining records
        self.writer.flush()
        stats.dates_processed = 1 if stats.symbols_processed > 0 else 0

        return stats

    def backfill(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[date] = None,
        end: Optional[date] = None,
        workers: Optional[int] = None,
    ) -> IngestStats:
        """Backfill kline data for a date range in parallel.

        Args:
            symbols: List of symbols to ingest. If None, uses default top symbols.
            start: Start date (inclusive). If None, uses 30 days ago.
            end: End date (inclusive). If None, uses yesterday.
            workers: Number of parallel workers. If None, uses config.max_workers.

        Returns:
            IngestStats with aggregated statistics
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        # Set defaults
        if workers is None:
            workers = self.config.max_workers

        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]

        date_range = self._get_date_range(start, end)

        # Aggregate stats
        total_stats = IngestStats()

        # Process dates in parallel
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all date ingestions
            futures = {
                executor.submit(self.ingest_date, d, symbols): d
                for d in date_range
            }

            # Collect results as they complete
            for future in as_completed(futures):
                target_date = futures[future]
                try:
                    date_stats = future.result()
                    total_stats.symbols_processed += date_stats.symbols_processed
                    total_stats.dates_processed += date_stats.dates_processed
                    total_stats.records_written += date_stats.records_written
                    total_stats.errors.extend(date_stats.errors)
                except Exception as e:
                    error_msg = f"Failed to process {target_date}: {e}"
                    total_stats.errors.append(error_msg)

        return total_stats

    def _get_date_range(self, start: Optional[date], end: Optional[date]) -> List[date]:
        """Generate a list of dates between start and end (inclusive).

        Args:
            start: Start date (inclusive). If None, uses 30 days ago.
            end: End date (inclusive). If None, uses yesterday.

        Returns:
            List of dates from start to end
        """
        if end is None:
            end = date.today() - timedelta(days=1)
        if start is None:
            start = end - timedelta(days=29)  # 30 days back

        date_list = []
        current = start
        while current <= end:
            date_list.append(current)
            current += timedelta(days=1)

        return date_list

    def close(self) -> None:
        """Close the ingestor and cleanup resources."""
        if not self._closed:
            self.writer.close()
            self.downloader.close()
            self._closed = True

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
