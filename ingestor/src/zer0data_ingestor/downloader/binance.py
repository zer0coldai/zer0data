"""Binance kline data downloader."""

import io
import zipfile
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import List, Optional
from urllib.parse import urljoin

import requests


class BinanceKlineDownloader:
    """Download kline data from Binance data vision."""

    BASE_URL = "https://data.binance.vision/data/futures/"
    EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    def __init__(self, timeout: int = 30):
        """Initialize downloader.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "zer0data-ingestor/0.1.0"})

    @lru_cache(maxsize=1)
    def list_perpetual_symbols(self) -> List[str]:
        """List all USDT perpetual symbols from Binance.

        Returns:
            List of symbol names (e.g., ["BTCUSDT", "ETHUSDT", ...])

        Raises:
            requests.RequestException: If API request fails
        """
        response = self.session.get(
            self.EXCHANGE_INFO_URL, timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()

        symbols = []
        for symbol_info in data.get("symbols", []):
            # Filter for USDT-margined perpetual contracts
            if (
                symbol_info.get("quoteAsset") == "USDT"
                and symbol_info.get("contractType") == "PERPETUAL"
                and symbol_info.get("status") == "TRADING"
            ):
                symbols.append(symbol_info["symbol"])

        return sorted(symbols)

    def download_daily_klines(
        self,
        symbol: str,
        date: date,
        interval: str = "1m",
    ) -> List[dict]:
        """Download daily kline data for a symbol.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            date: Date to download
            interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d)

        Returns:
            List of kline records as dicts with keys:
            - open_time: int (milliseconds timestamp)
            - open: str
            - high: str
            - low: str
            - close: str
            - volume: str
            - close_time: int (milliseconds timestamp)
            - quote_volume: str
            - trades: int
            - taker_buy_base: str
            - taker_buy_quote: str

        Raises:
            requests.RequestException: If download fails
            ValueError: If data format is invalid
        """
        # Build URL for daily klines
        # Format: https://data.binance.vision/data/futures/daily/klines/{symbol}/{interval}/
        #         {symbol}-{interval}-{YYYY-MM-DD}.zip
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{symbol}-{interval}-{date_str}.zip"

        url = urljoin(
            self.BASE_URL,
            f"daily/klines/{symbol}/{interval}/{filename}",
        )

        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        # Extract and parse CSV from ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                csv_file = zip_file.open(zip_file.namelist()[0])
                content = csv_file.read().decode("utf-8")

                klines = []
                for line in content.strip().split("\n"):
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) < 12:
                        continue

                    klines.append(
                        {
                            "open_time": int(parts[0]),
                            "open": parts[1],
                            "high": parts[2],
                            "low": parts[3],
                            "close": parts[4],
                            "volume": parts[5],
                            "close_time": int(parts[6]),
                            "quote_volume": parts[7],
                            "trades": int(parts[8]),
                            "taker_buy_base": parts[9],
                            "taker_buy_quote": parts[10],
                        }
                    )

                return klines

        except (zipfile.BadZipFile, KeyError, IndexError) as e:
            raise ValueError(f"Invalid data format for {symbol} on {date_str}: {e}")

    def get_available_dates(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: str = "1m",
    ) -> List[date]:
        """Check which dates have available kline data.

        Uses HEAD requests to check file existence without downloading.

        Args:
            symbol: Trading pair symbol
            start: Start date (inclusive)
            end: End date (inclusive)
            interval: Kline interval

        Returns:
            List of dates with available data, sorted ascending

        Raises:
            requests.RequestException: If request fails
        """
        available = []
        current = start
        delta = timedelta(days=1)

        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            filename = f"{symbol}-{interval}-{date_str}.zip"

            url = urljoin(
                self.BASE_URL,
                f"daily/klines/{symbol}/{interval}/{filename}",
            )

            try:
                response = self.session.head(url, timeout=self.timeout)
                if response.status_code == 200:
                    available.append(current)
            except requests.RequestException:
                # Skip unavailable dates
                pass

            current += delta

        return available

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self) -> "BinanceKlineDownloader":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
