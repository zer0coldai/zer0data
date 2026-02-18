from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from zer0data_ingestor.fetcher.sources.coinmetrics import build_factor_dataframe
from zer0data_ingestor.fetcher.sources.coinmetrics import flush_batch


def test_build_factor_dataframe_drops_non_numeric_values() -> None:
    csv_text = (
        "time,CapMrktEstUSD,ReferenceRate,Note\n"
        "2026-02-15,139059502.2038,0.6328712519,ok\n"
        "2026-02-16,,0.6522499227,bad\n"
        "2026-02-17,not-a-number,0.6492553905,test\n"
    )

    df, stats = build_factor_dataframe(symbol="0g", csv_text=csv_text)

    assert list(df.columns) == [
        "symbol",
        "datetime",
        "factor_name",
        "factor_value",
        "source",
    ]
    assert (df["symbol"] == "0g").all()
    assert (df["source"] == "coinmetrics").all()
    assert stats.dropped_non_numeric == 5

    pairs = set(zip(df["factor_name"], df["factor_value"]))
    assert ("CapMrktEstUSD", 139059502.2038) in pairs
    assert ("ReferenceRate", 0.6328712519) in pairs
    assert ("ReferenceRate", 0.6522499227) in pairs
    assert ("ReferenceRate", 0.6492553905) in pairs
    assert not any(name == "Note" for name in df["factor_name"])


def test_flush_batch_passes_max_partitions_setting() -> None:
    mock_client = MagicMock()
    batch = [
        pd.DataFrame(
            [
                {
                    "symbol": "btc",
                    "datetime": pd.Timestamp("2026-01-01T00:00:00Z"),
                    "factor_name": "ReferenceRateUSD",
                    "factor_value": 100.0,
                    "source": "coinmetrics",
                }
            ]
        )
    ]

    written = flush_batch(
        mock_client,
        batch,
        max_partitions_per_insert_block=1000,
    )

    assert written == 1
    mock_client.insert_df.assert_called_once()
    settings = mock_client.insert_df.call_args.kwargs["settings"]
    assert settings["max_partitions_per_insert_block"] == 1000
