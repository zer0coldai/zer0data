"""Run a direct SDK query for 2025 1h data of six symbols."""

from __future__ import annotations

import os
import sys
from datetime import datetime

from zer0data import Client


def main() -> int:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = os.getenv("CLICKHOUSE_DB", "zer0data")
    interval = os.getenv("INTERVAL", "1h")
    symbols = os.getenv(
        "SYMBOLS",
        "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT",
    ).split(",")
    start = datetime.fromisoformat(os.getenv("START", "2025-01-01"))
    end = datetime.fromisoformat(os.getenv("END", "2025-12-31T23:00:00"))

    try:
        with Client(host=host, port=port, database=database) as client:
            df = client.kline.query(
                symbols=symbols,
                interval=interval,
                start=start,
                end=end,
            )

        print(f"rows={df.height}")
        if df.height == 0:
            print("No rows returned.")
            return 0

        counts = df.select(["symbol"]).group_by("symbol").len().sort("symbol")
        print(counts)
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
