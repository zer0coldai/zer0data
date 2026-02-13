import pytest
import pandas as pd
from zer0data_ingestor.cleaner.kline import KlineCleaner, CleanResult
from zer0data_ingestor.writer.clickhouse import KlineRecord


def test_kline_cleaner_defaults_to_one_minute_interval():
    """Default cleaner interval should match 1m kline data."""
    cleaner = KlineCleaner()
    assert cleaner.interval_ms == 60000


def test_kline_cleaner_removes_duplicates():
    """Test that duplicate records are removed, keeping first occurrence."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,  # duplicate
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=2000, close_time=2059,
                   open_price=50100.0, high_price=50200.0, low_price=50000.0,
                   close_price=50150.0, volume=200.0, quote_volume=10000000.0,
                   trades_count=2000, taker_buy_volume=100.0, taker_buy_quote_volume=5000000.0),
    ]

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(records)

    assert len(result.cleaned_records) == 2
    assert result.stats.duplicates_removed == 1
    assert result.cleaned_records[0].open_time == 1000
    assert result.cleaned_records[1].open_time == 2000

    # Verify interval is preserved
    assert result.cleaned_records[0].interval == "1m"
    assert result.cleaned_records[1].interval == "1m"


def test_kline_cleaner_validates_ohlc_logic():
    """Test that invalid OHLC records are removed."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=2000, close_time=2059,
                   open_price=50100.0, high_price=50000.0, low_price=50200.0,  # Invalid: low > high
                   close_price=50150.0, volume=200.0, quote_volume=10000000.0,
                   trades_count=2000, taker_buy_volume=100.0, taker_buy_quote_volume=5000000.0),
        KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
                   open_price=50000.0, high_price=-100.0, low_price=49900.0,  # Invalid: negative price
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    ]

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(records)

    assert len(result.cleaned_records) == 1
    assert result.stats.invalid_records_removed == 2
    # Check that validation errors contain the expected error types
    error_text = " ".join(result.stats.validation_errors)
    assert "high < low" in error_text or ("negative" in error_text or "non-positive" in error_text)

    # Verify interval is preserved in cleaned record
    assert result.cleaned_records[0].interval == "1m"


def test_kline_cleaner_fills_time_gaps():
    """Test that time gaps are filled using forward fill."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        # Gap: missing 2000, should be filled
        KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
                   open_price=50200.0, high_price=50300.0, low_price=50100.0,
                   close_price=50250.0, volume=300.0, quote_volume=15000000.0,
                   trades_count=3000, taker_buy_volume=150.0, taker_buy_quote_volume=7500000.0),
    ]

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(records)

    # Should have 3 records: original 2 + 1 filled gap
    assert len(result.cleaned_records) == 3
    assert result.stats.gaps_filled == 1

    # Check the filled record
    filled = result.cleaned_records[1]
    assert filled.open_time == 2000
    # Filled with forward fill from previous record
    assert filled.open_price == 50050.0  # Previous close
    assert filled.close_price == 50050.0
    assert filled.volume == 0.0

    # Verify interval is preserved in all records
    assert all(r.interval == "1m" for r in result.cleaned_records)


def test_kline_cleaner_does_not_use_iterrows(monkeypatch):
    """Cleaner conversion should avoid DataFrame.iterrows for better performance."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
                   open_price=50200.0, high_price=50300.0, low_price=50100.0,
                   close_price=50250.0, volume=300.0, quote_volume=15000000.0,
                   trades_count=3000, taker_buy_volume=150.0, taker_buy_quote_volume=7500000.0),
    ]

    def _fail_iterrows(self):  # pragma: no cover - intentional failure hook
        raise AssertionError("iterrows should not be used")

    monkeypatch.setattr(pd.DataFrame, "iterrows", _fail_iterrows)

    cleaner = KlineCleaner(interval_ms=1000)
    result = cleaner.clean(records)
    assert len(result.cleaned_records) == 3

    # Verify interval is preserved in all records
    assert all(r.interval == "1m" for r in result.cleaned_records)
