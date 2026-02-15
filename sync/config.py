"""Ops configuration loader.

Loads sync/download configuration from a YAML file with typed dataclasses.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Dataclass definitions
# ---------------------------------------------------------------------------

@dataclass
class RemoteConfig:
    """Remote server connection settings."""

    host: str
    data_dir: str


@dataclass
class LocalConfig:
    """Local paths for data, state and logs."""

    data_dir: str
    state_dir: str
    log_dir: str
    project_root: str


@dataclass
class R2Config:
    """Cloudflare R2 settings."""

    bucket: str = "zer0data"
    prefix: str = "download"
    transfers: int = 8


@dataclass
class StorageConfig:
    """Transfer backend configuration."""

    type: str = "r2"  # "r2" or "rsync"
    r2: R2Config = field(default_factory=R2Config)


@dataclass
class DownloadConfig:
    """Which symbols / intervals / market to download."""

    symbols: list[str] = field(default_factory=list)
    intervals: list[str] = field(default_factory=list)
    market: str = "um"


@dataclass
class ClickHouseConfig:
    """ClickHouse connection settings (ops-level)."""

    host: str = "localhost"
    port: int = 8123
    database: str = "zer0data"
    username: str = "default"
    password: str = ""


@dataclass
class ScheduleConfig:
    """Schedule times for remote download and local sync."""

    remote_time: str = "01:30"
    local_time: str = "10:00"


@dataclass
class OpsConfig:
    """Top-level ops configuration."""

    remote: RemoteConfig
    local: LocalConfig
    storage: StorageConfig
    download: DownloadConfig
    clickhouse: ClickHouseConfig
    schedule: ScheduleConfig

    # ---- factories --------------------------------------------------------

    @classmethod
    def load(cls, path: Path | str | None = None) -> OpsConfig:
        """Load configuration from a YAML file.

        Resolution order for *path*:
        1. Explicit argument
        2. ``ZER0DATA_OPS_CONFIG`` environment variable
        3. ``config.yaml`` in the same directory as this module
        """
        if path is None:
            env_path = os.getenv("ZER0DATA_OPS_CONFIG")
            if env_path:
                path = Path(env_path)
            else:
                path = Path(__file__).parent / "config.yaml"
        else:
            path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path) as fh:
            raw: dict[str, Any] = yaml.safe_load(fh)

        return cls._from_dict(raw)

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> OpsConfig:
        """Build config from parsed YAML dict.

        Environment variables take precedence over YAML values so that
        Docker Compose can override paths without changing the file:

        - ``REMOTE_HOST``, ``REMOTE_DATA_DIR``
        - ``LOCAL_DATA_DIR``, ``LOCAL_STATE_DIR``, ``LOCAL_LOG_DIR``,
          ``LOCAL_PROJECT_ROOT``
        - ``STORAGE_TYPE``
        - ``R2_BUCKET``, ``R2_PREFIX``, ``R2_TRANSFERS``
        - ``CLICKHOUSE_HOST``, ``CLICKHOUSE_PORT``, ``CLICKHOUSE_DB``,
          ``CLICKHOUSE_USER``, ``CLICKHOUSE_PASSWORD``
        """
        remote_data = data.get("remote", {})
        local_data = data.get("local", {})
        storage_data = data.get("storage", {})
        r2_data = storage_data.get("r2", {})
        download_data = data.get("download", {})
        ch_data = data.get("clickhouse", {})
        sched_data = data.get("schedule", {})

        def _env(key: str, fallback: Any) -> str:
            return os.getenv(key, fallback)

        return cls(
            remote=RemoteConfig(
                host=_env("REMOTE_HOST", remote_data.get("host", "")),
                data_dir=_env("REMOTE_DATA_DIR", remote_data.get("data_dir", "")),
            ),
            local=LocalConfig(
                data_dir=_env("LOCAL_DATA_DIR", local_data["data_dir"]),
                state_dir=_env("LOCAL_STATE_DIR", local_data["state_dir"]),
                log_dir=_env("LOCAL_LOG_DIR", local_data["log_dir"]),
                project_root=_env("LOCAL_PROJECT_ROOT", local_data["project_root"]),
            ),
            storage=StorageConfig(
                type=_env("STORAGE_TYPE", storage_data.get("type", "r2")),
                r2=R2Config(
                    bucket=_env("R2_BUCKET", r2_data.get("bucket", "zer0data")),
                    prefix=_env("R2_PREFIX", r2_data.get("prefix", "download")),
                    transfers=int(_env("R2_TRANSFERS", r2_data.get("transfers", 8))),
                ),
            ),
            download=DownloadConfig(
                symbols=download_data.get("symbols", []),
                intervals=download_data.get("intervals", []),
                market=download_data.get("market", "um"),
            ),
            clickhouse=ClickHouseConfig(
                host=_env("CLICKHOUSE_HOST", ch_data.get("host", "localhost")),
                port=int(_env("CLICKHOUSE_PORT", ch_data.get("port", 8123))),
                database=_env("CLICKHOUSE_DB", ch_data.get("database", "zer0data")),
                username=_env("CLICKHOUSE_USER", ch_data.get("username", "default")),
                password=_env("CLICKHOUSE_PASSWORD", ch_data.get("password", "")),
            ),
            schedule=ScheduleConfig(
                remote_time=sched_data.get("remote_time", "01:30"),
                local_time=sched_data.get("local_time", "10:00"),
            ),
        )
