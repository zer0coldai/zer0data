import pytest
from zer0data_ingestor.cleaner.kline import KlineCleaner, CleanResult
from zer0data_ingestor.writer.clickhouse import KlineRecord


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

    cleaner = KlineCleaner()
    result = cleaner.clean(records)

    assert len(result.cleaned_records) == 2
    assert result.stats.duplicates_removed == 1
    assert result.cleaned_records[0].open_time == 1000
    assert result.cleaned_records[1].open_time == 2000
