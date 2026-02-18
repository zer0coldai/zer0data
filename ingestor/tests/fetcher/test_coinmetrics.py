from __future__ import annotations

from zer0data_ingestor.fetcher.sources.coinmetrics import build_factor_dataframe


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
