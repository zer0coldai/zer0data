from __future__ import annotations

import json
import logging
import socket
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Any, Iterable

import clickhouse_connect

logger = logging.getLogger(__name__)


def setup_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def http_get_text(url: str, timeout: int, retries: int) -> tuple[int, str, int]:
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


def http_get_json(url: str, timeout: int, retries: int) -> tuple[int, dict[str, Any], int]:
    status, text, latency_ms = http_get_text(url=url, timeout=timeout, retries=retries)
    try:
        return status, json.loads(text), latency_ms
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from {url}: {exc}") from exc


def get_clickhouse_client(args: Any):
    return clickhouse_connect.get_client(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
        database=args.clickhouse_db,
    )


def log_csv_preview(name: str, text: str, head: int = 3, tail: int = 3) -> None:
    lines = text.splitlines()
    logger.info("[%s] preview head=%d tail=%d total_lines=%d", name, head, tail, len(lines))
    for idx, line in enumerate(lines[:head], start=1):
        logger.info("[%s][head:%d] %s", name, idx, line)

    if tail > 0 and len(lines) > head:
        tail_buf: Iterable[str] = deque(lines[-tail:], maxlen=tail)
        for idx, line in enumerate(tail_buf, start=1):
            logger.info("[%s][tail:%d] %s", name, idx, line)
