"""
Symbols Service - Query symbol metadata from raw_exchange_info payload
"""

import clickhouse_connect
import polars as pl


class SymbolService:
    """Service for querying symbol metadata."""

    def __init__(self, client: clickhouse_connect.driver.client.Client, database: str):
        self._client = client
        self._database = database

    def query(self, market: str = "um") -> pl.DataFrame:
        """Query latest symbols from raw_exchange_info for one market."""
        normalized_market = self._validate_market(market)

        query = f"""
        WITH latest_payload AS (
            SELECT payload
            FROM {self._database}.raw_exchange_info
            WHERE market = '{normalized_market}'
              AND endpoint = 'exchangeInfo'
              AND status_code = 200
            ORDER BY pulled_at DESC
            LIMIT 1
        )
        SELECT
            JSONExtractString(symbol_raw, 'symbol') AS symbol,
            JSONExtractInt(symbol_raw, 'onboardDate') AS onboardDate,
            JSONExtractInt(symbol_raw, 'deliveryDate') AS deliveryDate,
            JSONExtractString(symbol_raw, 'underlyingType') AS underlyingType,
            JSONExtractString(symbol_raw, 'status') AS status
        FROM (
            SELECT arrayJoin(JSONExtractArrayRaw(payload, 'symbols')) AS symbol_raw
            FROM latest_payload
        )
        ORDER BY symbol
        """

        result = self._client.query(query)
        return pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")

    def _validate_market(self, market: str) -> str:
        if market not in {"spot", "um", "cm"}:
            raise ValueError(f"invalid market: {market}")
        return market
