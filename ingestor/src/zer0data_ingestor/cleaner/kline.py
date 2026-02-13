"""Kline data cleaner."""

import pandas as pd
from dataclasses import dataclass, field
from typing import List
from zer0data_ingestor.writer.clickhouse import KlineRecord


@dataclass
class CleaningStats:
    """Statistics for data cleaning operations."""
    duplicates_removed: int = 0
    gaps_filled: int = 0
    invalid_records_removed: int = 0
    validation_errors: List[str] = field(default_factory=list)


@dataclass
class CleanResult:
    """Result of cleaning operation."""
    cleaned_records: List[KlineRecord]
    stats: CleaningStats


class KlineCleaner:
    """Cleaner for kline data."""

    def __init__(self, interval_ms: int = 60000):
        """Initialize the cleaner.

        Args:
            interval_ms: Expected time interval between records in milliseconds (default: 60000ms = 1 minute)
        """
        self.interval_ms = interval_ms

    def _validate_record(self, record: KlineRecord) -> tuple[bool, list[str]]:
        """Validate a kline record.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Check for negative prices
        if record.open_price <= 0 or record.high_price <= 0 or record.low_price <= 0 or record.close_price <= 0:
            errors.append(f"non-positive price at {record.open_time}")

        # Check OHLC logic: high >= max(open, close) and low <= min(open, close)
        if record.high_price < max(record.open_price, record.close_price):
            errors.append(f"high < max(open, close) at {record.open_time}")

        if record.low_price > min(record.open_price, record.close_price):
            errors.append(f"low > min(open, close) at {record.open_time}")

        if record.high_price < record.low_price:
            errors.append(f"high < low at {record.open_time}")

        # Check volume
        if record.volume < 0:
            errors.append(f"negative volume at {record.open_time}")

        return (len(errors) == 0, errors)

    def _convert_to_dataframe(self, records: List[KlineRecord]) -> pd.DataFrame:
        """Convert list of KlineRecord to pandas DataFrame.

        Args:
            records: List of kline records

        Returns:
            DataFrame with records as rows
        """
        if not records:
            return pd.DataFrame()

        rows = [
            {
                "open_time": r.open_time,
                "close_time": r.close_time,
                "open_price": r.open_price,
                "high_price": r.high_price,
                "low_price": r.low_price,
                "close_price": r.close_price,
                "volume": r.volume,
                "quote_volume": r.quote_volume,
                "trades_count": r.trades_count,
                "taker_buy_volume": r.taker_buy_volume,
                "taker_buy_quote_volume": r.taker_buy_quote_volume,
                "interval": r.interval,
            }
            for r in records
        ]
        return pd.DataFrame.from_records(rows, index="open_time")

    def _convert_from_dataframe(self, df: pd.DataFrame, symbol: str, interval: str = "1m") -> List[KlineRecord]:
        """Convert pandas DataFrame to list of KlineRecord.

        Args:
            df: DataFrame with kline data
            symbol: Symbol for all records
            interval: Interval string for all records

        Returns:
            List of KlineRecord objects
        """
        records = []
        for row in df.itertuples(index=True, name=None):
            (
                open_time,
                close_time,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                quote_volume,
                trades_count,
                taker_buy_volume,
                taker_buy_quote_volume,
                interval_from_df,
            ) = row
            record = KlineRecord(
                symbol=symbol,
                open_time=int(open_time),
                close_time=int(close_time),
                open_price=float(open_price),
                high_price=float(high_price),
                low_price=float(low_price),
                close_price=float(close_price),
                volume=float(volume),
                quote_volume=float(quote_volume),
                trades_count=int(trades_count),
                taker_buy_volume=float(taker_buy_volume),
                taker_buy_quote_volume=float(taker_buy_quote_volume),
                interval=interval_from_df,
            )
            records.append(record)
        return records

    def _fill_gaps(self, records: List[KlineRecord], stats: CleaningStats) -> List[KlineRecord]:
        """Fill time gaps using pandas forward fill.

        Args:
            records: List of kline records (already deduplicated and validated)
            stats: CleaningStats to update with gaps_filled count

        Returns:
            List of records with gaps filled
        """
        if len(records) < 2:
            return records

        # Get symbol from first record
        symbol = records[0].symbol

        # Convert to DataFrame
        df = self._convert_to_dataframe(records)

        # Use configured interval
        interval_ms = self.interval_ms

        # Create complete time range
        min_time = df.index.min()
        max_time = df.index.max()

        # Create expected index with proper step
        expected_range = pd.RangeIndex(
            start=min_time,
            stop=max_time + 1,
            step=interval_ms
        )

        # Count gaps before filling
        original_count = len(df)

        # Reindex to fill gaps and forward fill
        df_reindexed = df.reindex(expected_range)

        # Forward fill quote columns first
        quote_cols = ['quote_volume', 'trades_count', 'taker_buy_volume', 'taker_buy_quote_volume']
        df_reindexed[quote_cols] = df_reindexed[quote_cols].ffill()

        # Forward fill interval column
        df_reindexed['interval'] = df_reindexed['interval'].ffill()

        # Forward fill close_price first
        df_reindexed['close_price'] = df_reindexed['close_price'].ffill()

        # For filled rows, set all OHLC to the previous close_price
        filled_mask = df_reindexed['open_price'].isna()
        if filled_mask.any():
            df_reindexed.loc[filled_mask, 'open_price'] = df_reindexed.loc[filled_mask, 'close_price']
            df_reindexed.loc[filled_mask, 'high_price'] = df_reindexed.loc[filled_mask, 'close_price']
            df_reindexed.loc[filled_mask, 'low_price'] = df_reindexed.loc[filled_mask, 'close_price']

        # Fill volume with 0 for gaps
        df_reindexed['volume'] = df_reindexed['volume'].fillna(0.0)

        # Calculate close_time for filled rows (open_time + interval - 1)
        filled_mask = df_reindexed['close_time'].isna()
        if filled_mask.any():
            df_reindexed.loc[filled_mask, 'close_time'] = (
                df_reindexed[filled_mask].index + interval_ms - 1
            )

        # Update stats
        stats.gaps_filled = len(df_reindexed) - original_count

        # Convert back to records, passing interval from first record
        interval = records[0].interval if records else "1m"
        return self._convert_from_dataframe(df_reindexed, symbol, interval)

    def clean(self, records: List[KlineRecord]) -> CleanResult:
        """Clean kline records by removing duplicates, validating, and filling gaps.

        Args:
            records: List of kline records to clean

        Returns:
            CleanResult with cleaned records and statistics
        """
        stats = CleaningStats()

        # Remove duplicates, keep first occurrence
        seen_times = set()
        cleaned = []
        for record in records:
            if record.open_time in seen_times:
                stats.duplicates_removed += 1
            else:
                seen_times.add(record.open_time)
                cleaned.append(record)

        # Validate records
        validated = []
        for record in cleaned:
            is_valid, errors = self._validate_record(record)
            if is_valid:
                validated.append(record)
            else:
                stats.invalid_records_removed += 1
                stats.validation_errors.extend(errors)

        # Fill time gaps
        result = self._fill_gaps(validated, stats)

        return CleanResult(cleaned_records=result, stats=stats)
