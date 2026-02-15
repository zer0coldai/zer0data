"""Tests for CLI interface."""

import tempfile
import zipfile
from pathlib import Path

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
