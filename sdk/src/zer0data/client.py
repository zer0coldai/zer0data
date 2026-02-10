"""
zer0data Client - Main interface for data access
"""

from dataclasses import dataclass
from typing import Optional
import clickhouse_connect

from zer0data.kline import KlineService


@dataclass
class ClientConfig:
    """Configuration for zer0data client"""

    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "zer0data"


class Client:
    """Client for accessing zer0data from ClickHouse"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "zer0data",
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
        self.config = ClientConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self._client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )
        self._kline: Optional[KlineService] = None

    @property
    def kline(self) -> KlineService:
        """Get the kline service"""
        if self._kline is None:
            self._kline = KlineService(self._client, self.config.database)
        return self._kline

    def close(self):
        """Close the client connection"""
        self._client.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False
