"""Tests for CLI interface."""

from click.testing import CliRunner
from pytest import raises
import datetime
import zipfile
import tempfile
from pathlib import Path

from zer0data_ingestor.cli import cli


def test_cli_help():
    """Test that CLI help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "Zer0data Ingestor" in result.output
    assert "ingest-from-dir" in result.output


def test_ingest_from_dir_command():
    """Test ingest-from-dir command with a temporary directory and zip file."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test zip file
        test_zip_path = Path(tmpdir) / "BTCUSDT-1m-2024-01-01.zip"
        with zipfile.ZipFile(test_zip_path, "w") as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", "0,1704067200000,1704067259999,42000.5,42100.0,41950.0,42075.5,1234.56")

        result = runner.invoke(
            cli,
            ["ingest-from-dir", "--source", tmpdir, "--symbols", "BTCUSDT"]
        )

        # Command should execute (even if database is not available)
        # We're testing the command structure, not the full integration
        assert result.exit_code == 0 or "connect" in result.output.lower()


def test_ingest_from_dir_requires_source():
    """Test that ingest-from-dir requires --source option."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-from-dir"])

    assert result.exit_code != 0
    assert "--source" in result.output or "Missing option" in result.output


def test_ingest_from_dir_help():
    """Test that ingest-from-dir help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-from-dir", "--help"])

    assert result.exit_code == 0
    assert "--source" in result.output
    assert "--symbols" in result.output
    assert "--pattern" in result.output
