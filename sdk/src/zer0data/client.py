"""
zer0data Client - Main interface for data access
"""

from dataclasses import dataclass
from datetime import datetime
import os
from typing import Optional, Union
import clickhouse_connect
import polars as pl

from zer0data.kline import KlineService
from zer0data.symbols import SymbolService
from zer0data.factor import FactorService


@dataclass
class ClientConfig:
    """Configuration for zer0data client"""

    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "zer0data"

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Build config from environment variables."""
        return cls(
            host=os.getenv("ZER0DATA_CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("ZER0DATA_CLICKHOUSE_PORT", "8123")),
            username=os.getenv("ZER0DATA_CLICKHOUSE_USERNAME", "default"),
            password=os.getenv("ZER0DATA_CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("ZER0DATA_CLICKHOUSE_DATABASE", "zer0data"),
        )


class Client:
    """Client for accessing zer0data from ClickHouse"""

    @classmethod
    def from_env(cls) -> "Client":
        """Create client using ClickHouse settings from environment variables."""
        return cls()

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        """
        Initialize ClickHouse client

        Args:
            host: ClickHouse host
            port: ClickHouse port
            database: Database name
            username: Username
            password: Password
        """
        env_config = ClientConfig.from_env()
        self.config = ClientConfig(
            host=host if host is not None else env_config.host,
            port=port if port is not None else env_config.port,
            username=username if username is not None else env_config.username,
            password=password if password is not None else env_config.password,
            database=database if database is not None else env_config.database,
        )
        self._client = clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            username=self.config.username,
            password=self.config.password,
        )
        self._kline: Optional[KlineService] = None
        self._symbols: Optional[SymbolService] = None
        self._factors: Optional[FactorService] = None

    @property
    def kline(self) -> KlineService:
        """Get the kline service"""
        if self._kline is None:
            self._kline = KlineService(self._client, self.config.database)
        return self._kline

    @property
    def symbols(self) -> SymbolService:
        """Get the symbols service"""
        if self._symbols is None:
            self._symbols = SymbolService(self._client, self.config.database)
        return self._symbols

    @property
    def factors(self) -> FactorService:
        """Get the factors service"""
        if self._factors is None:
            self._factors = FactorService(self._client, self.config.database)
        return self._factors

    def close(self):
        """Close the client connection"""
        self._client.close()

    def get_klines(
        self,
        symbols: Union[str, list[str]],
        interval: str = "1m",
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """Direct SDK entrypoint for querying kline data."""
        return self.kline.query(
            symbols=symbols,
            interval=interval,
            start=start,
            end=end,
            limit=limit,
        )

    def get_symbols(
        self,
        market: str = "um",
        quote_asset: Optional[str] = None,
        exclude_stable_base: bool = False,
    ) -> pl.DataFrame:
        """Direct SDK entrypoint for querying symbol metadata."""
        if quote_asset is None and not exclude_stable_base:
            return self.symbols.query(market=market)
        if quote_asset is not None and not exclude_stable_base:
            return self.symbols.query(market=market, quote_asset=quote_asset)
        return self.symbols.query(
            market=market,
            quote_asset=quote_asset,
            exclude_stable_base=exclude_stable_base,
        )

    def get_factors(
        self,
        symbols: Union[str, list[str]],
        factor_names: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        format: str = "long",
    ) -> pl.DataFrame:
        """Direct SDK entrypoint for querying factor data."""
        return self.factors.query(
            symbols=symbols,
            factor_names=factor_names,
            start=start,
            end=end,
            format=format,
        )

    def write_factors(self, data, source: str = "sdk") -> int:
        """Direct SDK entrypoint for writing factor data."""
        return self.factors.write(data=data, source=source)

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False
