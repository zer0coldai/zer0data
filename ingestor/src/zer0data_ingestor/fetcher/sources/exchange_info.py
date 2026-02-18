from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from zer0data_ingestor.fetcher.core import (
    get_clickhouse_client,
    http_get_text,
    setup_logging,
)
from zer0data_ingestor.fetcher.types import FetchResult

MARKET_URLS = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "um": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "cm": "https://dapi.binance.com/dapi/v1/exchangeInfo",
}

TABLE_NAME = "raw_exchange_info"
ENDPOINT = "exchangeInfo"
logger = logging.getLogger(__name__)


def _env(*keys: str, default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Binance exchangeInfo and store raw JSON into ClickHouse."
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["um"],
        choices=["spot", "um", "cm"],
        help="Markets to fetch (default: um).",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds (default: 20).")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry count (default: 3).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate JSON only, do not insert into ClickHouse.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO).",
    )
    parser.add_argument(
        "--clickhouse-host",
        default=_env("CLICKHOUSE_HOST", "ZER0DATA_CLICKHOUSE_HOST", default="localhost"),
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(_env("CLICKHOUSE_PORT", "ZER0DATA_CLICKHOUSE_PORT", default="8123")),
    )
    parser.add_argument(
        "--clickhouse-db",
        default=_env("CLICKHOUSE_DB", "ZER0DATA_CLICKHOUSE_DATABASE", default="zer0data"),
    )
    parser.add_argument(
        "--clickhouse-user",
        default=_env("CLICKHOUSE_USER", "ZER0DATA_CLICKHOUSE_USERNAME", default="default"),
    )
    parser.add_argument(
        "--clickhouse-password",
        default=_env("CLICKHOUSE_PASSWORD", "ZER0DATA_CLICKHOUSE_PASSWORD", default=""),
    )
    return parser.parse_args()


def ensure_table_exists(client, database: str) -> None:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{TABLE_NAME}
    (
        pulled_at DateTime64(3, 'UTC'),
        market LowCardinality(String),
        endpoint LowCardinality(String),
        source_url String,
        status_code UInt16,
        latency_ms UInt32,
        payload String CODEC(ZSTD(6)),
        payload_hash FixedString(64),
        ingest_version UInt64 DEFAULT toUnixTimestamp64Milli(now64(3)),
        err_msg Nullable(String)
    )
    ENGINE = ReplacingMergeTree(ingest_version)
    PARTITION BY toYYYYMM(pulled_at)
    ORDER BY (market, endpoint, pulled_at, payload_hash)
    SETTINGS index_granularity = 8192
    """
    client.command(create_sql)


def insert_payloads(client, rows: list[tuple[datetime, str, str, str, int, int, str, str, None]]) -> None:
    client.insert(
        table=TABLE_NAME,
        data=rows,
        column_names=[
            "pulled_at",
            "market",
            "endpoint",
            "source_url",
            "status_code",
            "latency_ms",
            "payload",
            "payload_hash",
            "err_msg",
        ],
    )


def run(args: argparse.Namespace) -> FetchResult:
    setup_logging(args.log_level)
    pulled_at = datetime.now(timezone.utc)
    rows: list[tuple[datetime, str, str, str, int, int, str, str, None]] = []

    logger.info(
        "Start fetch: markets=%s timeout=%ss retries=%d dry_run=%s",
        ",".join(args.markets),
        args.timeout,
        args.retries,
        args.dry_run,
    )

    for market in args.markets:
        url = MARKET_URLS[market]
        logger.info("Fetching %s from %s", market, url)
        status_code, payload, latency_ms = http_get_text(url, timeout=args.timeout, retries=args.retries)
        parsed = json.loads(payload)
        symbols_count = len(parsed.get("symbols", []))
        assets_count = len(parsed.get("assets", []))
        payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        logger.info(
            "[%s] status=%d latency_ms=%d symbols=%d assets=%d hash=%s...",
            market,
            status_code,
            latency_ms,
            symbols_count,
            assets_count,
            payload_hash[:12],
        )

        rows.append(
            (
                pulled_at,
                market,
                ENDPOINT,
                url,
                status_code,
                latency_ms,
                payload,
                payload_hash,
                None,
            )
        )

    if args.dry_run:
        logger.info("Dry run only. Skip ClickHouse insert.")
        return FetchResult(files_total=len(args.markets), files_ok=len(args.markets), rows_written=0, errors=0)

    client = get_clickhouse_client(args)
    try:
        ensure_table_exists(client, args.clickhouse_db)
        insert_payloads(client, rows)
    finally:
        client.close()

    return FetchResult(files_total=len(args.markets), files_ok=len(args.markets), rows_written=len(rows), errors=0)


def main() -> int:
    args = parse_args()
    result = run(args)
    logger.info(
        "Done: files_total=%d files_ok=%d rows_written=%d errors=%d",
        result.files_total,
        result.files_ok,
        result.rows_written,
        result.errors,
    )
    return 0
