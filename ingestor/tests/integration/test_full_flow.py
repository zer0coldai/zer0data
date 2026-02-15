"""Integration test for full parse -> clean -> ingest flow."""

import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.ingestor import KlineIngestor


def test_full_flow():
    """Complete flow: simulated download -> parse -> clean -> write."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_data = (
            b"1704067200000,42000.00,42100.00,41900.00,42050.00,"
            b"1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
        )
        zip_path = Path(tmp_dir) / "BTCUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        with patch("zer0data_ingestor.ingestor.ClickHouseWriter") as mock_writer_cls:
            mock_writer = MagicMock()
            mock_writer_cls.return_value = mock_writer

            config = IngestorConfig(
                clickhouse=ClickHouseConfig(
                    host="localhost",
                    port=8123,
                    database="test_db",
                ),
            )

            with KlineIngestor(config) as ingestor:
                stats = ingestor.ingest_from_directory(tmp_dir, symbols=["BTCUSDT"])

            assert stats.symbols_processed >= 1
            assert stats.records_written >= 1
            assert stats.files_processed >= 1
            assert mock_writer.write_df.call_count > 0
            mock_writer.close.assert_called_once()

            # Verify the written DataFrame has the right structure.
            written_df = mock_writer.write_df.call_args[0][0]
            assert "symbol" in written_df.columns
            assert "open_time" in written_df.columns
            assert written_df.iloc[0]["symbol"] == "BTCUSDT"
