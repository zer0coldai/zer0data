"""
Kline Service - Query kline/candlestick data from ClickHouse
"""

from datetime import datetime, timezone
from typing import Optional, Union
import polars as pl
import clickhouse_connect


class KlineService:
    """Service for querying kline (candlestick) data"""

    def __init__(self, client: clickhouse_connect.driver.client.Client, database: str):
        """
        Initialize kline service

        Args:
            client: ClickHouse client instance
            database: Database name
        """
        self._client = client
        self._database = database

    def query(
        self,
        symbols: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Query kline data

        Args:
            symbols: Symbol(s) to query (e.g., 'BTCUSDT' or ['BTCUSDT', 'ETHUSDT'])
            start: Start timestamp (ISO format or Unix timestamp in ms)
            end: End timestamp (ISO format or Unix timestamp in ms)
            limit: Maximum number of rows to return

        Returns:
            Polars DataFrame with kline data

        Raises:
            ValueError: If symbols is empty
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        if not symbols:
            raise ValueError("symbols must be a non-empty string or list of strings")

        # Build WHERE clause
        where_conditions = [f"symbol IN {tuple(symbols)}"]

        if start is not None:
            where_conditions.append(f"open_time >= {self._parse_timestamp(start)}")

        if end is not None:
            where_conditions.append(f"open_time <= {self._parse_timestamp(end)}")

        where_clause = " AND ".join(where_conditions)

        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        # Build and execute query
        query = f"""
        SELECT
            symbol,
            open_time,
            close_time,
            open_price AS open,
            high_price AS high,
            low_price AS low,
            close_price AS close,
            volume,
            quote_volume,
            trades_count,
            taker_buy_volume,
            taker_buy_quote_volume
        FROM {self._database}.klines
        WHERE {where_clause}
        ORDER BY symbol, open_time
        {limit_clause}
        """

        result = self._client.query(query)
        return pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")

    def query_stream(
        self,
        symbols: Union[str, list[str]],
        start: Optional[str] = None,
        end: Optional[str] = None,
    ):
        """
        Query kline data as a stream

        Args:
            symbols: Symbol(s) to query
            start: Start timestamp
            end: End timestamp

        Returns:
            Stream of kline data

        Raises:
            NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError("query_stream is not yet implemented")

    def _parse_timestamp(self, timestamp: Union[str, int, datetime]) -> int:
        """
        Parse timestamp to Unix milliseconds

        Args:
            timestamp: ISO format string or Unix timestamp string

        Returns:
            Unix timestamp in milliseconds
        """
        if isinstance(timestamp, datetime):
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return int(timestamp.timestamp() * 1000)

        # If it's numeric, treat as milliseconds
        if isinstance(timestamp, int):
            return timestamp

        try:
            return int(timestamp)
        except (TypeError, ValueError):
            # Support ISO strings like "2024-01-01" and "2024-01-01T00:00:00Z"
            ts = str(timestamp)
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
