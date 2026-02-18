"""Tests for CLI interface."""

import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner

from zer0data_ingestor.cli import cli


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "Zer0data Ingestor" in result.output
    assert "ingest-from-dir" in result.output


def test_ingest_from_dir_command():
    """ingest-from-dir with a temp zip file should not crash on CLI level."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "BTCUSDT-1m-2024-01-01.zip"
        csv_data = (
            "1704067200000,42000.00,42100.00,41900.00,42050.00,"
            "1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
        )
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        result = runner.invoke(
            cli,
            ["ingest-from-dir", "--source", tmpdir, "--symbols", "BTCUSDT"],
        )

        # Command should execute (may fail at ClickHouse connection — that's OK).
        assert result.exit_code == 0 or "connect" in result.output.lower()


def test_ingest_from_dir_requires_source():
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-from-dir"])

    assert result.exit_code != 0
    assert "--source" in result.output or "Missing option" in result.output


def test_ingest_from_dir_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-from-dir", "--help"])

    assert result.exit_code == 0
    assert "--source" in result.output
    assert "--symbols" in result.output
    assert "--pattern" in result.output
    # --cleaner-interval-ms should be gone.
    assert "--cleaner-interval-ms" not in result.output


def test_cli_no_cleaner_interval_option():
    """Top-level help should NOT include the removed --cleaner-interval-ms option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "--cleaner-interval-ms" not in result.output


def test_cli_has_ingest_source_group():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "ingest-source" in result.output


def test_ingest_source_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-source", "--help"])

    assert result.exit_code == 0
    assert "exchange-info" in result.output
    assert "coinmetrics" in result.output


def test_ingest_source_exchange_info_calls_fetcher():
    runner = CliRunner()

    with patch("zer0data_ingestor.cli.run_exchange_info") as mock_run:
        mock_run.return_value = SimpleNamespace(
            files_total=1, files_ok=1, rows_written=1, errors=0
        )
        result = runner.invoke(
            cli,
            [
                "--clickhouse-host",
                "127.0.0.1",
                "--clickhouse-port",
                "9000",
                "--clickhouse-db",
                "testdb",
                "--clickhouse-user",
                "u1",
                "--clickhouse-password",
                "p1",
                "ingest-source",
                "exchange-info",
                "--markets",
                "um",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0
    called_args = mock_run.call_args[0][0]
    assert called_args.clickhouse_host == "127.0.0.1"
    assert called_args.clickhouse_port == 9000
    assert called_args.clickhouse_db == "testdb"
    assert called_args.clickhouse_user == "u1"
    assert called_args.clickhouse_password == "p1"
    assert called_args.markets == ["um"]
    assert called_args.dry_run is True


def test_ingest_source_coinmetrics_calls_fetcher():
    runner = CliRunner()

    with patch("zer0data_ingestor.cli.run_coinmetrics") as mock_run:
        mock_run.return_value = SimpleNamespace(
            files_total=2, files_ok=2, rows_written=100, errors=0
        )
        result = runner.invoke(
            cli,
            [
                "ingest-source",
                "coinmetrics",
                "--symbols",
                "btc",
                "--symbols",
                "eth",
                "--head",
                "2",
                "--tail",
                "2",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0
    called_args = mock_run.call_args[0][0]
    assert called_args.symbols == ["btc", "eth"]
    assert called_args.head == 2
    assert called_args.tail == 2
    assert called_args.dry_run is True


def test_ingest_source_exchange_info_handles_fetch_error():
    runner = CliRunner()

    with patch("zer0data_ingestor.cli.run_exchange_info", side_effect=RuntimeError("boom")):
        result = runner.invoke(cli, ["ingest-source", "exchange-info", "--markets", "um"])

    assert result.exit_code != 0
    assert "Source ingestion failed: boom" in result.output
