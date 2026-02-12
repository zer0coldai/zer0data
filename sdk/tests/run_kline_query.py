"""Directly runnable SDK query test script."""

from __future__ import annotations

import os
import sys
from datetime import datetime

import polars as pl
from zer0data import Client


def main() -> int:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = os.getenv("CLICKHOUSE_DB", "zer0data")
    symbol = os.getenv("SYMBOL", "BTCUSDT")
    start = datetime.fromisoformat(os.getenv("START", "2024-01-01"))
    end = datetime.fromisoformat(os.getenv("END", "2024-01-02"))

    print(f"Querying {symbol} from {start} to {end} on {host}:{port}/{database}")

    try:
        client = Client(host=host, port=port, database=database)
        df = client.kline.query(symbols=[symbol], start=start, end=end)
        print(f"rows={df.height}")
        if df.height > 0:
            max_rows = int(os.getenv("MAX_ROWS", "10"))
            out = df.head(max_rows)
            print(f"showing first {out.height} rows as DataFrame:")
            with pl.Config(tbl_cols=-1, tbl_rows=max_rows, tbl_width_chars=240):
                print(out)
        else:
            print("No rows returned.")
        client.close()
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
