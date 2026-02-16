"""Kline data parser for Binance zip files — DataFrame edition."""

import logging
import zipfile
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import pandas as pd

from zer0data_ingestor.constants import is_valid_interval
from zer0data_ingestor.schema import BINANCE_CSV_COLUMNS

logger = logging.getLogger(__name__)


def extract_interval_from_filename(filename: str) -> str:
    """Extract interval from a Binance kline filename.

    Binance filename format: SYMBOL-INTERVAL-DATE.zip
    Example: BTCUSDT-1h-2024-01-01.zip -> 1h

    Args:
        filename: The filename or path to extract interval from

    Returns:
        The extracted interval string (e.g., "1m", "1h", "1d")

    Raises:
        ValueError: If interval cannot be extracted or is not valid.

    Examples:
        >>> extract_interval_from_filename("BTCUSDT-1h-2024-01-01.zip")
        '1h'
        >>> extract_interval_from_filename("ETHUSDT-1d-2024-01-01.zip")
        '1d'
    """
    name = Path(filename).stem
    parts = name.split("-")
    if len(parts) >= 2:
        interval = parts[1]
        if is_valid_interval(interval):
            return interval

    raise ValueError(
        f"Cannot extract valid interval from filename: {filename}"
    )


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date from a Binance kline filename.

    Binance filename formats:
    - Daily: SYMBOL-INTERVAL-YYYY-MM-DD.zip -> 2024-01-01
    - Monthly: SYMBOL-INTERVAL-YYYY-MM.zip -> 2025-01-01 (first day of month)

    Args:
        filename: The filename or path to extract date from

    Returns:
        The extracted date string (e.g., "2024-01-01") or None if not found

    Examples:
        >>> extract_date_from_filename("BTCUSDT-1h-2024-01-01.zip")
        '2024-01-01'
        >>> extract_date_from_filename("BTCUSDT-1h-2025-01.zip")
        '2025-01-01'
    """
    name = Path(filename).stem
    parts = name.split("-")

    # Try daily format: SYMBOL-INTERVAL-YYYY-MM-DD
    if len(parts) >= 5:
        try:
            date_str = f"{parts[2]}-{parts[3]}-{parts[4]}"
            pd.to_datetime(date_str)
            return date_str
        except (ValueError, IndexError):
            pass

    # Try monthly format: SYMBOL-INTERVAL-YYYY-MM
    if len(parts) >= 4:
        try:
            date_str = f"{parts[2]}-{parts[3]}-01"
            pd.to_datetime(date_str)
            return date_str
        except (ValueError, IndexError):
            pass

    return None


class KlineParser:
    """Parser for Binance kline data from zip files.

    Returns pandas DataFrames instead of individual records.
    """

    def parse_file(
        self,
        zip_path: str,
        symbol: str,
        interval: Optional[str] = None,
    ) -> pd.DataFrame:
        """Parse a single zip file containing kline CSV data.

        Args:
            zip_path: Path to the zip file
            symbol: Trading symbol (e.g., "BTCUSDT")
            interval: Optional interval override. If not provided, interval will be
                extracted from the filename.

        Returns:
            DataFrame with kline data and columns matching schema.KLINE_COLUMNS.
            Returns an empty DataFrame (with correct columns) when there is no data.

        Raises:
            FileNotFoundError: If zip file does not exist.
            ValueError: If zip file is corrupted or invalid.
        """
        path = Path(zip_path)
        if not path.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        if interval is None:
            interval = extract_interval_from_filename(zip_path)

        try:
            with zipfile.ZipFile(zip_path, mode="r") as zf:
                csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                if not csv_files:
                    return self._empty_dataframe()

                with zf.open(csv_files[0]) as csv_file:
                    # Read without strict dtypes so header rows don't cause
                    # cast errors.  We cast after cleaning.
                    df = pd.read_csv(
                        csv_file,
                        header=None,
                        names=BINANCE_CSV_COLUMNS,
                    )

            # Drop the Binance "ignore" column.
            df = df.drop(columns=["ignore"], errors="ignore")

            # Skip header row if present (Binance CSV sometimes has one).
            if not df.empty:
                first = df.iloc[0]["open_time"]
                if isinstance(first, str) and not first.isdigit():
                    df = df.iloc[1:].copy()

            # Cast columns to the correct types.
            df["open_time"] = pd.to_numeric(df["open_time"]).astype("int64")
            df["close_time"] = pd.to_numeric(df["close_time"]).astype("int64")
            df["trades_count"] = pd.to_numeric(df["trades_count"]).astype("int64")
            for col in [
                "open_price", "high_price", "low_price", "close_price",
                "volume", "quote_volume", "taker_buy_volume",
                "taker_buy_quote_volume",
            ]:
                df[col] = pd.to_numeric(df[col]).astype("float64")

            # Add symbol and interval columns
            df["symbol"] = symbol
            df["interval"] = interval

            return df

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid zip file: {zip_path}") from e
        except Exception as e:
            if isinstance(e, (FileNotFoundError, ValueError)):
                raise
            raise ValueError(f"Error parsing zip file: {e}") from e

    def parse_directory(
        self,
        dir_path: str,
        symbols: Optional[List[str]] = None,
        intervals: Optional[List[str]] = None,
        pattern: str = "**/*.zip",
    ) -> Iterator[Tuple[str, str, pd.DataFrame]]:
        """Parse all zip files in a directory.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter.
            intervals: Optional list of intervals to filter.
            pattern: Glob pattern for matching files (default: "**/*.zip")

        Yields:
            Tuples of (symbol, interval, DataFrame)

        Raises:
            FileNotFoundError: If directory does not exist.
        """
        for symbol, interval, df, _ in self.parse_directory_with_path(
            dir_path, symbols, intervals, pattern
        ):
            yield (symbol, interval, df)

    def parse_directory_with_path(
        self,
        dir_path: str,
        symbols: Optional[List[str]] = None,
        intervals: Optional[List[str]] = None,
        pattern: str = "**/*.zip",
    ) -> Iterator[Tuple[str, str, pd.DataFrame, str]]:
        """Parse all zip files in a directory, including file paths.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter.
            intervals: Optional list of intervals to filter.
            pattern: Glob pattern for matching files (default: "**/*.zip")

        Yields:
            Tuples of (symbol, interval, DataFrame, file_path)

        Raises:
            FileNotFoundError: If directory does not exist.
        """
        dir_p = Path(dir_path)
        if not dir_p.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")
        if not dir_p.is_dir():
            raise ValueError(f"Path is not a directory: {dir_path}")

        zip_files = sorted(dir_p.glob(pattern))
        symbol_filter = set(symbols) if symbols is not None else None
        interval_filter = set(intervals) if intervals is not None else None

        for zip_path in zip_files:
            file_symbol = zip_path.stem.partition("-")[0]

            if symbol_filter is not None:
                if file_symbol not in symbol_filter:
                    continue
                symbol = file_symbol
            else:
                symbol = file_symbol if file_symbol else "UNKNOWN"

            # Extract and validate interval from filename
            try:
                file_interval = extract_interval_from_filename(str(zip_path))
            except ValueError:
                logger.warning(
                    "Skipping file with unrecognisable interval: %s", zip_path
                )
                continue

            if interval_filter is not None and file_interval not in interval_filter:
                continue

            # Parse the zip file and yield DataFrame with file path
            try:
                df = self.parse_file(str(zip_path), symbol)
                if not df.empty:
                    yield (symbol, file_interval, df, str(zip_path))
            except (FileNotFoundError, ValueError) as exc:
                logger.warning("Skipping unparseable file %s: %s", zip_path, exc)
                continue

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_dataframe() -> pd.DataFrame:
        """Return an empty DataFrame with the expected kline columns."""
        from zer0data_ingestor.schema import KLINE_COLUMNS

        return pd.DataFrame(columns=KLINE_COLUMNS)
