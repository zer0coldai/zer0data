"""
Symbols Service - Query symbol metadata from raw_exchange_info payload
"""

import re
from typing import Optional

import clickhouse_connect
import polars as pl


class SymbolService:
    """Service for querying symbol metadata."""

    STABLE_SYMBOLS = [
        "BKRW",
        "USDC",
        "USDP",
        "TUSD",
        "BUSD",
        "FDUSD",
        "DAI",
        "EUR",
        "GBP",
        "USBP",
        "SUSD",
        "PAXG",
        "AEUR",
    ]

    def __init__(self, client: clickhouse_connect.driver.client.Client, database: str):
        self._client = client
        self._database = database

    def query(
        self,
        market: str = "um",
        quote_asset: Optional[str] = None,
        exclude_stable_base: bool = False,
    ) -> pl.DataFrame:
        """Query latest symbols from raw_exchange_info for one market."""
        normalized_market = self._validate_market(market)
        normalized_quote_asset = self._validate_quote_asset(quote_asset)
        quote_asset_filter = ""
        if normalized_quote_asset is not None:
            quote_asset_filter = (
                f"\n          AND JSONExtractString(symbol_raw, 'quoteAsset') = '{normalized_quote_asset}'"
            )
        stable_base_filter = ""
        if exclude_stable_base:
            stable_symbols_sql = ", ".join(f"'{symbol}'" for symbol in self.STABLE_SYMBOLS)
            stable_base_filter = (
                "\n          AND JSONExtractString(symbol_raw, 'baseAsset') "
                f"NOT IN ({stable_symbols_sql})"
            )

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
            JSONExtractString(symbol_raw, 'quoteAsset') AS quoteAsset,
            JSONExtractInt(symbol_raw, 'onboardDate') AS onboardDate,
            JSONExtractInt(symbol_raw, 'deliveryDate') AS deliveryDate,
            JSONExtractString(symbol_raw, 'underlyingType') AS underlyingType,
            JSONExtractString(symbol_raw, 'status') AS status
        FROM (
            SELECT arrayJoin(JSONExtractArrayRaw(payload, 'symbols')) AS symbol_raw
            FROM latest_payload
        )
        WHERE 1 = 1{quote_asset_filter}{stable_base_filter}
        ORDER BY symbol
        """

        result = self._client.query(query)
        return pl.DataFrame(result.result_rows, schema=result.column_names, orient="row")

    def _validate_market(self, market: str) -> str:
        if market not in {"spot", "um", "cm"}:
            raise ValueError(f"invalid market: {market}")
        return market

    def _validate_quote_asset(self, quote_asset: Optional[str]) -> Optional[str]:
        if quote_asset is None:
            return None

        normalized_quote_asset = quote_asset.strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{2,20}", normalized_quote_asset):
            raise ValueError(f"invalid quote_asset: {quote_asset}")
        return normalized_quote_asset
