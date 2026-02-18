from __future__ import annotations

import argparse
import io
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from zer0data_ingestor.fetcher.core import (
    get_clickhouse_client,
    http_get_json,
    http_get_text,
    log_csv_preview,
    setup_logging,
)
from zer0data_ingestor.fetcher.types import FetchResult

GITHUB_TREE_API = "https://api.github.com/repos/coinmetrics/data/git/trees/master?recursive=1"
RAW_BASE = "https://raw.githubusercontent.com/coinmetrics/data/master/"
TABLE_NAME = "factors"
logger = logging.getLogger(__name__)


@dataclass
class TransformStats:
    dropped_non_numeric: int = 0


def _env(*keys: str, default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch all CoinMetrics CSV files and ingest factors into ClickHouse."
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds (default: 30).")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry count (default: 3).")
    parser.add_argument("--head", type=int, default=3, help="Preview head lines per CSV (default: 3).")
    parser.add_argument("--tail", type=int, default=3, help="Preview tail lines per CSV (default: 3).")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Batch rows for ClickHouse insert.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional symbols to fetch (default: all symbols).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse only, do not insert into ClickHouse.",
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
        symbol LowCardinality(String),
        datetime DateTime64(3, 'UTC'),
        factor_name LowCardinality(String),
        factor_value Float64,
        source LowCardinality(String) DEFAULT 'coinmetrics',
        update_time DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(update_time)
    PARTITION BY toYYYYMM(datetime)
    ORDER BY (symbol, datetime, factor_name)
    SETTINGS index_granularity = 8192
    """
    client.command(create_sql)


def list_coinmetrics_csv_paths(timeout: int, retries: int) -> list[str]:
    _, payload, _ = http_get_json(GITHUB_TREE_API, timeout=timeout, retries=retries)
    csv_paths: list[str] = []
    for node in payload.get("tree", []):
        if not isinstance(node, dict):
            continue
        path = node.get("path")
        if isinstance(path, str) and path.startswith("csv/") and path.endswith(".csv"):
            csv_paths.append(path)
    return sorted(csv_paths)


def build_factor_dataframe(symbol: str, csv_text: str) -> tuple[pd.DataFrame, TransformStats]:
    source_df = pd.read_csv(io.StringIO(csv_text))
    if "time" not in source_df.columns:
        raise ValueError(f"CSV for {symbol} missing required 'time' column")

    metric_columns = [col for col in source_df.columns if col != "time"]
    if not metric_columns:
        return pd.DataFrame(columns=["symbol", "datetime", "factor_name", "factor_value", "source"]), TransformStats()

    melted = source_df.melt(
        id_vars=["time"],
        value_vars=metric_columns,
        var_name="factor_name",
        value_name="factor_value",
    )

    melted["datetime"] = pd.to_datetime(melted["time"], utc=True, errors="coerce")
    melted["factor_value"] = pd.to_numeric(melted["factor_value"], errors="coerce")

    invalid_mask = melted["datetime"].isna() | melted["factor_value"].isna()
    dropped_non_numeric = int(invalid_mask.sum())

    narrowed = melted.loc[~invalid_mask, ["datetime", "factor_name", "factor_value"]].copy()
    narrowed.insert(0, "symbol", symbol)
    narrowed["source"] = "coinmetrics"

    return narrowed, TransformStats(dropped_non_numeric=dropped_non_numeric)


def flush_batch(client, batch: list[pd.DataFrame]) -> int:
    if not batch:
        return 0
    merged = pd.concat(batch, ignore_index=True)
    merged["update_time"] = datetime.now(timezone.utc)
    client.insert_df(TABLE_NAME, merged[["symbol", "datetime", "factor_name", "factor_value", "source", "update_time"]])
    return len(merged)


def run(args: argparse.Namespace) -> FetchResult:
    setup_logging(args.log_level)
    all_paths = list_coinmetrics_csv_paths(timeout=args.timeout, retries=args.retries)

    if args.symbols:
        wanted = {s.lower() for s in args.symbols}
        csv_paths = [p for p in all_paths if p.split("/")[-1].removesuffix(".csv") in wanted]
    else:
        csv_paths = all_paths

    logger.info("CoinMetrics csv files to process: %d", len(csv_paths))

    client = None
    if not args.dry_run:
        client = get_clickhouse_client(args)
        ensure_table_exists(client, args.clickhouse_db)

    result = FetchResult(files_total=len(csv_paths))
    batch: list[pd.DataFrame] = []
    batch_rows = 0
    dropped_total = 0

    try:
        for index, path in enumerate(csv_paths, start=1):
            symbol = path.split("/")[-1].removesuffix(".csv")
            url = RAW_BASE + path

            try:
                _, csv_text, _ = http_get_text(url, timeout=args.timeout, retries=args.retries)
                log_csv_preview(path, csv_text, head=args.head, tail=args.tail)

                factor_df, stats = build_factor_dataframe(symbol=symbol, csv_text=csv_text)
                dropped_total += stats.dropped_non_numeric
                if stats.dropped_non_numeric > 0:
                    logger.warning("[%s] dropped_non_numeric=%d", path, stats.dropped_non_numeric)

                result.files_ok += 1
                if factor_df.empty:
                    continue

                if args.dry_run:
                    continue

                batch.append(factor_df)
                batch_rows += len(factor_df)

                if batch_rows >= args.batch_size:
                    written = flush_batch(client, batch)
                    result.rows_written += written
                    batch.clear()
                    batch_rows = 0
                    logger.info("Progress %d/%d rows_written=%d", index, len(csv_paths), result.rows_written)
            except Exception as exc:
                result.errors += 1
                logger.warning("[%s] failed: %s", path, exc)
                continue

        if not args.dry_run and batch:
            result.rows_written += flush_batch(client, batch)
    finally:
        if client is not None:
            client.close()

    logger.info(
        "CoinMetrics run done: files_total=%d files_ok=%d rows_written=%d errors=%d dropped_non_numeric=%d",
        result.files_total,
        result.files_ok,
        result.rows_written,
        result.errors,
        dropped_total,
    )
    return result


def main() -> int:
    args = parse_args()
    run(args)
    return 0
