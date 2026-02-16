#!/usr/bin/env python3
"""Fetch Binance exchangeInfo JSON and store raw payloads in ClickHouse."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import clickhouse_connect

MARKET_URLS = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "um": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "cm": "https://dapi.binance.com/dapi/v1/exchangeInfo",
}

TABLE_NAME = "raw_exchange_info"
ENDPOINT = "exchangeInfo"
logger = logging.getLogger("fetch_exchange_info")


def fetch_json(url: str, timeout: int, retries: int) -> tuple[int, str, int]:
    """Fetch JSON from URL with retry on transient network errors."""
    for attempt in range(retries):
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                payload = response.read().decode("utf-8")
                latency_ms = int((time.perf_counter() - start) * 1000)
                return response.status, payload, latency_ms
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Network error for {url}: {exc}") from exc
            wait_seconds = attempt + 1
            logger.warning(
                "Network error for %s (attempt %d/%d): %s; retry in %ss",
                url,
                attempt + 1,
                retries,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Unexpected retry flow for {url}")


def parse_args() -> argparse.Namespace:
    def _env(*keys: str, default: str) -> str:
        for key in keys:
            value = os.getenv(key)
            if value is not None and value != "":
                return value
        return default

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
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout seconds (default: 20).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retry count (default: 3).",
    )
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


def setup_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def ensure_table_exists(client: clickhouse_connect.driver.client.Client, database: str) -> None:
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


def insert_payloads(
    client: clickhouse_connect.driver.client.Client,
    rows: list[tuple[datetime, str, str, str, int, int, str, str, None]],
) -> None:
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


def main() -> int:
    args = parse_args()
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
    logger.info(
        "ClickHouse target host=%s port=%d db=%s user=%s table=%s",
        args.clickhouse_host,
        args.clickhouse_port,
        args.clickhouse_db,
        args.clickhouse_user,
        TABLE_NAME,
    )

    for market in args.markets:
        url = MARKET_URLS[market]
        logger.info("Fetching %s from %s", market, url)
        status_code, payload, latency_ms = fetch_json(url, timeout=args.timeout, retries=args.retries)

        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from {url}: {exc}") from exc

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
        return 0

    logger.info("Connecting ClickHouse")
    client = clickhouse_connect.get_client(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
        database=args.clickhouse_db,
    )
    try:
        logger.info("Ensuring table exists: %s.%s", args.clickhouse_db, TABLE_NAME)
        ensure_table_exists(client, args.clickhouse_db)
        logger.info("Inserting %d row(s)", len(rows))
        insert_payloads(client, rows)
    finally:
        client.close()
        logger.info("ClickHouse connection closed")

    logger.info("Inserted %d row(s) into %s.%s", len(rows), args.clickhouse_db, TABLE_NAME)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI safety net
        logger.error("Failed: %s", exc)
        raise SystemExit(1)
