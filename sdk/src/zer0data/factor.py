"""
Factor Service - Query factor data from ClickHouse
"""

from datetime import datetime, timezone
from typing import Optional, Union
import polars as pl
import clickhouse_connect


class FactorService:
    """Service for querying factor data from ClickHouse"""

    def __init__(self, client: clickhouse_connect.driver.client.Client, database: str):
        """
        Initialize factor service

        Args:
            client: ClickHouse client instance
            database: Database name
        """
        self._client = client
        self._database = database

    def query(
        self,
        symbols: Union[str, list[str]],
        factor_names: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        format: str = "long",
    ) -> pl.DataFrame:
        """
        Query factor data

        Args:
            symbols: Symbol(s) to query (e.g., 'BTCUSDT' or ['BTCUSDT', 'ETHUSDT'])
            factor_names: Factor name(s) to query (e.g., 'price_usd' or ['price_usd', 'volume']) - required
            start: Start timestamp (ISO format or Unix timestamp in ms)
            end: End timestamp (ISO format or Unix timestamp in ms)
            format: Output format, "long" or "wide", default "long"

        Returns:
            Polars DataFrame with factor data

        Raises:
            ValueError: If symbols or factor_names is empty, or format is invalid
        """
        normalized_symbols = self._normalize_symbols(symbols)
        normalized_factor_names = self._normalize_factor_names(factor_names)
        validated_format = self._validate_format(format)

        where_clause = self._build_where_clause(
            normalized_symbols, normalized_factor_names, start, end
        )

        order_clause = "ORDER BY symbol, datetime, factor_name"

        # Build and execute query
        query = f"""
        SELECT
            symbol,
            datetime,
            factor_name,
            factor_value
        FROM {self._database}.factors
        WHERE {where_clause}
        {order_clause}
        """

        result = self._client.query(query)
        df = pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")

        # Convert to wide format if requested
        if validated_format == "wide":
            df = df.pivot(
                index=["symbol", "datetime"],
                columns="factor_name",
                values="factor_value",
            )

        return df

    def _normalize_symbols(self, symbols: Union[str, list[str]]) -> list[str]:
        """Normalize and validate symbols input."""
        if isinstance(symbols, str):
            symbols = [symbols]
        if not symbols:
            raise ValueError("symbols must be a non-empty string or list of strings")
        return list(dict.fromkeys(symbols))

    def _normalize_factor_names(self, factor_names: Union[str, list[str]]) -> list[str]:
        """Normalize and validate factor_names input."""
        if isinstance(factor_names, str):
            factor_names = [factor_names]
        if not factor_names:
            raise ValueError("factor_names must be a non-empty string or list of strings")
        return list(dict.fromkeys(factor_names))

    def _validate_format(self, format: str) -> str:
        """Validate and normalize format parameter."""
        format_lower = format.lower()
        if format_lower not in {"long", "wide"}:
            raise ValueError(f"format must be 'long' or 'wide', got '{format}'")
        return format_lower

    def _build_where_clause(
        self,
        symbols: list[str],
        factor_names: list[str],
        start: Optional[Union[str, int, datetime]],
        end: Optional[Union[str, int, datetime]],
    ) -> str:
        """Build SQL WHERE clause for symbol, factor_name and time filtering."""
        quoted_symbols = ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)
        quoted_factor_names = ", ".join(
            "'" + f.replace("'", "''") + "'" for f in factor_names
        )
        where_conditions = [f"symbol IN ({quoted_symbols})"]
        where_conditions.append(f"factor_name IN ({quoted_factor_names})")

        if start is not None:
            where_conditions.append(
                f"datetime >= {self._to_datetime_expr(start)}"
            )

        if end is not None:
            where_conditions.append(
                f"datetime <= {self._to_datetime_expr(end)}"
            )

        return " AND ".join(where_conditions)

    def _to_datetime_expr(self, timestamp: Union[str, int, datetime]) -> str:
        """Convert supported timestamp input to ClickHouse UTC DateTime expression."""
        seconds = self._parse_timestamp_seconds(timestamp)
        return f"toDateTime({seconds}, 'UTC')"

    def _parse_timestamp_seconds(self, timestamp: Union[str, int, datetime]) -> int:
        """
        Parse timestamp to Unix seconds.

        Args:
            timestamp: ISO format string or Unix timestamp (seconds or milliseconds)

        Returns:
            Unix timestamp in seconds
        """
        if isinstance(timestamp, datetime):
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return int(timestamp.timestamp())

        # Numeric input: support seconds or milliseconds.
        if isinstance(timestamp, int):
            return timestamp // 1000 if timestamp > 10_000_000_000 else timestamp

        try:
            value = int(timestamp)
            return value // 1000 if value > 10_000_000_000 else value
        except (TypeError, ValueError):
            # Support ISO strings like "2024-01-01" and "2024-01-01T00:00:00Z"
            ts = str(timestamp)
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
