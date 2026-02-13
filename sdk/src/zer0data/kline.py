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
        normalized_symbols = self._normalize_symbols(symbols)
        where_clause = self._build_where_clause(normalized_symbols, start, end)

        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        order_clause = "ORDER BY open_time" if len(normalized_symbols) == 1 else "ORDER BY symbol, open_time"

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
        {order_clause}
        {limit_clause}
        """

        result = self._client.query(query)
        return pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")

    def query_stream(
        self,
        symbols: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        batch_size: int = 10000,
    ):
        """
        Query kline data as a stream

        Args:
            symbols: Symbol(s) to query
            start: Start timestamp
            end: End timestamp
            batch_size: Rows per batch

        Returns:
            Generator yielding Polars DataFrame batches
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        normalized_symbols = self._normalize_symbols(symbols)
        where_conditions = [self._build_where_clause(normalized_symbols, start, end)]

        last_symbol: Optional[str] = None
        last_open_time: Optional[int] = None

        while True:
            conditions = list(where_conditions)
            if last_symbol is not None and last_open_time is not None:
                escaped_symbol = last_symbol.replace("'", "''")
                conditions.append(
                    f"(symbol > '{escaped_symbol}' OR "
                    f"(symbol = '{escaped_symbol}' AND open_time > {last_open_time}))"
                )

            where_clause = " AND ".join(conditions)
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
            LIMIT {batch_size}
            """

            result = self._client.query(query)
            if not result.result_rows:
                break

            batch = pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")
            yield batch

            if len(result.result_rows) < batch_size:
                break

            last_row = result.result_rows[-1]
            last_symbol = str(last_row[0])
            last_open_time = int(last_row[1])

    def _normalize_symbols(self, symbols: Union[str, list[str]]) -> list[str]:
        """Normalize and validate symbols input."""
        if isinstance(symbols, str):
            symbols = [symbols]
        if not symbols:
            raise ValueError("symbols must be a non-empty string or list of strings")
        return list(dict.fromkeys(symbols))

    def _build_where_clause(
        self,
        symbols: list[str],
        start: Optional[Union[str, int, datetime]],
        end: Optional[Union[str, int, datetime]],
    ) -> str:
        """Build SQL WHERE clause for symbol and time filtering."""
        quoted_symbols = ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)
        where_conditions = [f"symbol IN ({quoted_symbols})"]

        if start is not None:
            where_conditions.append(f"open_time >= {self._parse_timestamp(start)}")

        if end is not None:
            where_conditions.append(f"open_time <= {self._parse_timestamp(end)}")

        return " AND ".join(where_conditions)

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
