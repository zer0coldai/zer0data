"""Tests for KlineCleaner — DataFrame edition."""

import pandas as pd
import pytest

from zer0data_ingestor.cleaner.kline import KlineCleaner, CleanResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows, interval="1m"):
    """Build a kline DataFrame from a list of row dicts (open_time required)."""
    defaults = {
        "symbol": "BTCUSDT",
        "close_time": 0,
        "open_price": 50000.0,
        "high_price": 50100.0,
        "low_price": 49900.0,
        "close_price": 50050.0,
        "volume": 100.0,
        "quote_volume": 5000000.0,
        "trades_count": 1000,
        "taker_buy_volume": 50.0,
        "taker_buy_quote_volume": 2500000.0,
        "interval": interval,
    }
    records = []
    for row in rows:
        rec = {**defaults, **row}
        records.append(rec)
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_defaults_to_one_minute_interval():
    cleaner = KlineCleaner()
    assert cleaner.interval_ms == 60_000


def test_removes_duplicates():
    df = _make_df([
        {"open_time": 1000, "close_time": 1059},
        {"open_time": 1000, "close_time": 1059},  # duplicate
        {"open_time": 2000, "close_time": 2059},
    ])

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(df)

    assert len(result.cleaned_df) == 2
    assert result.stats.duplicates_removed == 1
    assert list(result.cleaned_df["open_time"]) == [1000, 2000]


def test_validates_ohlc_logic():
    df = _make_df([
        {"open_time": 1000, "close_time": 1059},  # valid
        {
            "open_time": 2000, "close_time": 2059,
            "high_price": 50000.0, "low_price": 50200.0,  # invalid: low > high
        },
        {
            "open_time": 3000, "close_time": 3059,
            "high_price": -100.0,  # invalid: negative price
        },
    ])

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(df)

    assert len(result.cleaned_df) == 1
    assert result.stats.invalid_records_removed == 2
    error_text = " ".join(result.stats.validation_errors)
    assert "high < low" in error_text or "non-positive" in error_text


def test_fills_time_gaps():
    df = _make_df([
        {"open_time": 1000, "close_time": 1059, "close_price": 50050.0},
        # Gap: missing 2000
        {
            "open_time": 3000, "close_time": 3059,
            "open_price": 50200.0, "high_price": 50300.0,
            "low_price": 50100.0, "close_price": 50250.0,
        },
    ])

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(df)

    assert len(result.cleaned_df) == 3
    assert result.stats.gaps_filled == 1

    filled = result.cleaned_df.iloc[1]
    assert filled["open_time"] == 2000
    # Price columns forward-filled from previous close.
    assert filled["open_price"] == 50050.0
    assert filled["close_price"] == 50050.0
    assert filled["high_price"] == 50050.0
    assert filled["low_price"] == 50050.0
    # Volume columns filled with 0 (no trading in the gap).
    assert filled["volume"] == 0.0
    assert filled["quote_volume"] == 0.0
    assert filled["trades_count"] == 0
    assert filled["taker_buy_volume"] == 0.0
    assert filled["taker_buy_quote_volume"] == 0.0


def test_gap_fill_volume_columns_are_zero():
    """Regression: gap-filled rows must have 0 for all volume/trade columns."""
    df = _make_df([
        {
            "open_time": 1000, "close_time": 1059,
            "volume": 500.0, "quote_volume": 1000.0,
            "trades_count": 42,
            "taker_buy_volume": 200.0, "taker_buy_quote_volume": 400.0,
        },
        {
            "open_time": 3000, "close_time": 3059,
            "open_price": 50000.0, "high_price": 50100.0,
            "low_price": 49900.0, "close_price": 50050.0,
        },
    ])

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(df)
    filled = result.cleaned_df.iloc[1]

    assert filled["volume"] == 0.0
    assert filled["quote_volume"] == 0.0
    assert filled["trades_count"] == 0
    assert filled["taker_buy_volume"] == 0.0
    assert filled["taker_buy_quote_volume"] == 0.0


def test_empty_dataframe():
    df = pd.DataFrame()
    cleaner = KlineCleaner()
    result = cleaner.clean(df)
    assert result.cleaned_df.empty
    assert result.stats.duplicates_removed == 0


def test_single_record():
    df = _make_df([{"open_time": 1000, "close_time": 1059}])
    cleaner = KlineCleaner()
    result = cleaner.clean(df)
    assert len(result.cleaned_df) == 1


def test_interval_preserved():
    df = _make_df(
        [{"open_time": 1000, "close_time": 1059}],
        interval="1h",
    )
    cleaner = KlineCleaner(interval_ms=3_600_000)
    result = cleaner.clean(df)
    assert result.cleaned_df.iloc[0]["interval"] == "1h"
