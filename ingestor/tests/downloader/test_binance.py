"""Tests for Binance kline downloader."""

from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests

from zer0data_ingestor.downloader.binance import BinanceKlineDownloader


class TestListPerpetualSymbols:
    """Tests for list_perpetual_symbols method."""

    @pytest.fixture
    def mock_exchange_info_response(self):
        """Mock exchange info API response."""
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "BNBUSDT",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "BTCUSD_PERP",
                    "quoteAsset": "USD",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "contractType": "CURRENT_QUARTER",
                    "status": "TRADING",
                },
                {
                    "symbol": "SOLUSDT",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "BREAK",
                },
            ]
        }

    def test_list_perpetual_symbols(self, mock_exchange_info_response):
        """Test listing perpetual symbols filters correctly."""
        downloader = BinanceKlineDownloader()

        mock_response = Mock()
        mock_response.json.return_value = mock_exchange_info_response

        with patch.object(downloader.session, "get", return_value=mock_response):
            symbols = downloader.list_perpetual_symbols()

        assert symbols == ["BNBUSDT", "BTCUSDT", "ETHUSDT"]

    def test_list_perpetual_symbols_api_error(self):
        """Test listing perpetual symbols handles API errors."""
        downloader = BinanceKlineDownloader()

        with patch.object(
            downloader.session,
            "get",
            side_effect=requests.RequestException("API Error"),
        ):
            with pytest.raises(requests.RequestException):
                downloader.list_perpetual_symbols()

    def test_list_perpetual_symbols_empty_response(self):
        """Test listing perpetual symbols with empty response."""
        downloader = BinanceKlineDownloader()

        mock_response = Mock()
        mock_response.json.return_value = {"symbols": []}

        with patch.object(downloader.session, "get", return_value=mock_response):
            symbols = downloader.list_perpetual_symbols()

        assert symbols == []

    def test_list_perpetual_symbols_cached(self, mock_exchange_info_response):
        """Test that list_perpetual_symbols uses cache."""
        downloader = BinanceKlineDownloader()

        mock_response = Mock()
        mock_response.json.return_value = mock_exchange_info_response

        with patch.object(downloader.session, "get", return_value=mock_response) as mock_get:
            # First call
            symbols1 = downloader.list_perpetual_symbols()
            # Second call should use cache
            symbols2 = downloader.list_perpetual_symbols()

        # Should only call API once due to caching
        assert mock_get.call_count == 1
        assert symbols1 == symbols2 == ["BNBUSDT", "BTCUSDT", "ETHUSDT"]


class TestDownloadDailyKlines:
    """Tests for download_daily_klines method."""

    @pytest.fixture
    def sample_csv_data(self):
        """Sample kline CSV data from ZIP."""
        return (
            "1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,"
            "1704070799999,42050000.00,5000,500.25,42025000.25,0\n"
            "1704070800000,42050.00,42200.00,42000.00,42150.00,1200.3,"
            "1704074399999,50540000.00,6000,600.15,50270000.15,0\n"
        )

    @pytest.fixture
    def mock_zip_response(self, sample_csv_data):
        """Mock ZIP file response."""
        # Create a ZIP file in memory
        import io
        import zipfile

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("BTCUSDT-1m-2024-01-01.csv", sample_csv_data)
        zip_buffer.seek(0)

        mock_response = Mock()
        zip_bytes = zip_buffer.getvalue()
        mock_response.content = zip_bytes
        mock_response.status_code = 200
        # Mock raise_for_status to do nothing (success)
        mock_response.raise_for_status = Mock()
        return mock_response

    def test_download_daily_klines_success(self, mock_zip_response):
        """Test successful download and parsing of daily klines."""
        downloader = BinanceKlineDownloader()
        test_date = date(2024, 1, 1)

        with patch.object(downloader.session, "get", return_value=mock_zip_response):
            klines = downloader.download_daily_klines("BTCUSDT", test_date)

        assert len(klines) == 2
        assert klines[0]["open_time"] == 1704067200000
        assert klines[0]["open"] == "42000.00"
        assert klines[0]["high"] == "42100.00"
        assert klines[0]["low"] == "41900.00"
        assert klines[0]["close"] == "42050.00"
        assert klines[0]["volume"] == "1000.5"
        assert klines[0]["trades"] == 5000
        assert klines[1]["open_time"] == 1704070800000
        assert klines[1]["trades"] == 6000

    def test_download_daily_klines_custom_interval(self, mock_zip_response):
        """Test download with custom interval."""
        downloader = BinanceKlineDownloader()
        test_date = date(2024, 1, 1)

        with patch.object(downloader.session, "get", return_value=mock_zip_response):
            klines = downloader.download_daily_klines("BTCUSDT", test_date, interval="5m")

        assert len(klines) == 2

    def test_download_daily_klines_http_error(self):
        """Test download handles HTTP errors."""
        downloader = BinanceKlineDownloader()
        test_date = date(2024, 1, 1)

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with patch.object(downloader.session, "get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                downloader.download_daily_klines("INVALID", test_date)

    def test_download_daily_klines_invalid_zip(self):
        """Test download handles invalid ZIP data."""
        downloader = BinanceKlineDownloader()
        test_date = date(2024, 1, 1)

        mock_response = Mock()
        mock_response.content = b"not a valid zip file"
        mock_response.status_code = 200

        with patch.object(downloader.session, "get", return_value=mock_response):
            with pytest.raises(ValueError, match="Invalid data format"):
                downloader.download_daily_klines("BTCUSDT", test_date)

    def test_download_daily_klines_empty_file(self):
        """Test download handles empty CSV file."""
        import io
        import zipfile

        downloader = BinanceKlineDownloader()
        test_date = date(2024, 1, 1)

        # Create ZIP with empty CSV
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("BTCUSDT-1m-2024-01-01.csv", "")
        zip_buffer.seek(0)

        mock_response = Mock()
        mock_response.content = zip_buffer.getvalue()
        mock_response.status_code = 200

        with patch.object(downloader.session, "get", return_value=mock_response):
            klines = downloader.download_daily_klines("BTCUSDT", test_date)

        assert klines == []


class TestGetAvailableDates:
    """Tests for get_available_dates method."""

    def test_get_available_dates_all_found(self):
        """Test getting available dates when all dates exist."""
        downloader = BinanceKlineDownloader()
        start = date(2024, 1, 1)
        end = date(2024, 1, 3)

        def head_mock(url, timeout=None):
            mock_response = Mock()
            mock_response.status_code = 200
            return mock_response

        with patch.object(downloader.session, "head", side_effect=head_mock):
            available = downloader.get_available_dates("BTCUSDT", start, end)

        assert len(available) == 3
        assert available == [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]

    def test_get_available_dates_partial(self):
        """Test getting available dates with some missing."""
        downloader = BinanceKlineDownloader()
        start = date(2024, 1, 1)
        end = date(2024, 1, 3)

        call_count = 0

        def head_mock(url, timeout=None):
            nonlocal call_count
            call_count += 1
            mock_response = Mock()
            # Only return 200 for first and third dates
            mock_response.status_code = 200 if call_count in [1, 3] else 404
            return mock_response

        with patch.object(downloader.session, "head", side_effect=head_mock):
            available = downloader.get_available_dates("BTCUSDT", start, end)

        assert available == [date(2024, 1, 1), date(2024, 1, 3)]

    def test_get_available_dates_custom_interval(self):
        """Test getting available dates with custom interval."""
        downloader = BinanceKlineDownloader()
        start = date(2024, 1, 1)
        end = date(2024, 1, 1)

        def head_mock(url, timeout=None):
            mock_response = Mock()
            mock_response.status_code = 200
            return mock_response

        with patch.object(downloader.session, "head", side_effect=head_mock) as mock_head:
            downloader.get_available_dates("BTCUSDT", start, end, interval="5m")

        # Verify URL includes interval
        url = mock_head.call_args[0][0]
        assert "/5m/" in url

    def test_get_available_dates_handles_errors(self):
        """Test getting available dates handles network errors gracefully."""
        downloader = BinanceKlineDownloader()
        start = date(2024, 1, 1)
        end = date(2024, 1, 2)

        def head_mock(url, timeout=None):
            raise requests.RequestException("Network error")

        with patch.object(downloader.session, "head", side_effect=head_mock):
            available = downloader.get_available_dates("BTCUSDT", start, end)

        # Should return empty list on errors
        assert available == []


class TestContextManager:
    """Tests for context manager functionality."""

    def test_context_manager(self):
        """Test using downloader as context manager."""
        with BinanceKlineDownloader() as downloader:
            assert downloader is not None
            assert downloader.session is not None

    def test_close(self):
        """Test explicit close."""
        downloader = BinanceKlineDownloader()
        session = downloader.session
        downloader.close()
        # Session close method should be called
        assert session is not None
