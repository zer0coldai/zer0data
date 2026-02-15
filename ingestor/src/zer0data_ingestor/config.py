"""Configuration management."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ClickHouseConfig:
    """ClickHouse connection config."""

    host: str = "localhost"
    port: int = 8123
    database: str = "zer0data"
    username: Optional[str] = None
    password: Optional[str] = None

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Load from environment variables."""
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "zer0data"),
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
        )


@dataclass
class IngestorConfig:
    """Main ingestor configuration."""

    clickhouse: ClickHouseConfig

    @classmethod
    def from_env(cls) -> "IngestorConfig":
        """Load from environment variables."""
        return cls(
            clickhouse=ClickHouseConfig.from_env(),
        )
