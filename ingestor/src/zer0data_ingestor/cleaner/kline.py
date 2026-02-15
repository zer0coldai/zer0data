"""Kline data cleaner — DataFrame edition."""

import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from zer0data_ingestor.schema import PRICE_COLUMNS, VOLUME_COLUMNS

logger = logging.getLogger(__name__)


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

    cleaned_df: pd.DataFrame
    stats: CleaningStats


class KlineCleaner:
    """Cleaner for kline data.

    Operates entirely on pandas DataFrames — no per-row dataclass conversion.
    """

    def __init__(self, interval_ms: int = 60_000):
        """Initialize the cleaner.

        Args:
            interval_ms: Expected time interval between records in milliseconds.
        """
        self.interval_ms = interval_ms

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def clean(self, df: pd.DataFrame) -> CleanResult:
        """Clean kline DataFrame: deduplicate, validate, fill gaps.

        Args:
            df: DataFrame with kline data (must contain open_time, OHLCV columns).

        Returns:
            CleanResult with the cleaned DataFrame and statistics.
        """
        stats = CleaningStats()

        if df.empty:
            return CleanResult(cleaned_df=df, stats=stats)

        # 1. Remove duplicates (keep first occurrence by open_time)
        before = len(df)
        df = df.drop_duplicates(subset="open_time", keep="first")
        stats.duplicates_removed = before - len(df)

        # 2. Validate records
        df, stats = self._validate(df, stats)

        # 3. Fill time gaps
        df = self._fill_gaps(df, stats)

        return CleanResult(cleaned_df=df, stats=stats)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate(df: pd.DataFrame, stats: CleaningStats) -> tuple[pd.DataFrame, CleaningStats]:
        """Remove rows that violate OHLC or volume constraints."""
        if df.empty:
            return df, stats

        positive_prices = (
            (df["open_price"] > 0)
            & (df["high_price"] > 0)
            & (df["low_price"] > 0)
            & (df["close_price"] > 0)
        )
        high_ge_oc = df["high_price"] >= df[["open_price", "close_price"]].max(axis=1)
        low_le_oc = df["low_price"] <= df[["open_price", "close_price"]].min(axis=1)
        high_ge_low = df["high_price"] >= df["low_price"]
        non_neg_volume = df["volume"] >= 0

        valid_mask = positive_prices & high_ge_oc & low_le_oc & high_ge_low & non_neg_volume

        invalid_count = int((~valid_mask).sum())
        if invalid_count > 0:
            # Collect human-readable error summaries for the first few invalid rows.
            invalid_rows = df[~valid_mask]
            for _, row in invalid_rows.head(10).iterrows():
                errors: list[str] = []
                if row["open_price"] <= 0 or row["high_price"] <= 0 or row["low_price"] <= 0 or row["close_price"] <= 0:
                    errors.append(f"non-positive price at {row['open_time']}")
                if row["high_price"] < max(row["open_price"], row["close_price"]):
                    errors.append(f"high < max(open, close) at {row['open_time']}")
                if row["low_price"] > min(row["open_price"], row["close_price"]):
                    errors.append(f"low > min(open, close) at {row['open_time']}")
                if row["high_price"] < row["low_price"]:
                    errors.append(f"high < low at {row['open_time']}")
                if row["volume"] < 0:
                    errors.append(f"negative volume at {row['open_time']}")
                stats.validation_errors.extend(errors)

            stats.invalid_records_removed += invalid_count
            df = df[valid_mask].copy()

        return df, stats

    def _fill_gaps(self, df: pd.DataFrame, stats: CleaningStats) -> pd.DataFrame:
        """Fill time gaps in the kline data.

        * Price columns are forward-filled (flat candle at previous close).
        * Volume / trade-count columns are filled with 0 (no trading in the gap).
        """
        if len(df) < 2:
            return df

        df = df.sort_values("open_time").reset_index(drop=True)

        min_time = int(df["open_time"].iloc[0])
        max_time = int(df["open_time"].iloc[-1])

        expected_times = pd.RangeIndex(start=min_time, stop=max_time + 1, step=self.interval_ms)

        if len(expected_times) == len(df):
            # No gaps — fast path.
            return df

        original_count = len(df)

        # Use open_time as index for reindex-based gap filling.
        df = df.set_index("open_time")
        df = df.reindex(expected_times)
        df.index.name = "open_time"

        # Identify which rows were newly inserted (gaps).
        gap_mask = df["symbol"].isna()

        # Forward-fill metadata columns.
        df["symbol"] = df["symbol"].ffill()
        df["interval"] = df["interval"].ffill()

        # Forward-fill price columns (flat candle at previous close).
        df["close_price"] = df["close_price"].ffill()
        for col in ["open_price", "high_price", "low_price"]:
            df.loc[gap_mask, col] = df.loc[gap_mask, "close_price"]

        # Volume and trade columns → 0 for gaps.
        for col in VOLUME_COLUMNS + ["trades_count"]:
            df[col] = df[col].fillna(0)

        # Compute close_time for gap rows: open_time + interval_ms − 1.
        close_gap = df["close_time"].isna()
        if close_gap.any():
            df.loc[close_gap, "close_time"] = (
                df.index[close_gap].to_series().values + self.interval_ms - 1
            )

        # Ensure int columns stay int after fillna.
        df["close_time"] = df["close_time"].astype("int64")
        df["trades_count"] = df["trades_count"].astype("int64")

        stats.gaps_filled += len(df) - original_count

        # Restore open_time as a regular column.
        df = df.reset_index()

        return df
