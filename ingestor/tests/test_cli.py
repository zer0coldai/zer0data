"""Tests for CLI interface."""

from click.testing import CliRunner
from pytest import raises
import datetime

from zer0data_ingestor.cli import cli


def test_cli_help():
    """Test that CLI help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "Zer0data Ingestor" in result.output
    assert "backfill" in result.output
    assert "ingest-daily" in result.output
    assert "check-missing" in result.output


def test_cli_backfill_help():
    """Test that backfill command help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--help"])

    assert result.exit_code == 0
    assert "--symbols" in result.output
    assert "--all-symbols" in result.output
    assert "--start-date" in result.output
    assert "--end-date" in result.output
    assert "--workers" in result.output


def test_cli_ingest_daily_help():
    """Test that ingest-daily command help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest-daily", "--help"])

    assert result.exit_code == 0
    assert "--date" in result.output


def test_cli_check_missing_help():
    """Test that check-missing command help displays correctly."""
    runner = CliRunner()
    result = runner.invoke(cli, ["check-missing", "--help"])

    assert result.exit_code == 0
    assert "--symbols" in result.output
    assert "--start" in result.output
    assert "--end" in result.output


def test_cli_backfill_requires_args():
    """Test that backfill command requires either --symbols or --all-symbols."""
    runner = CliRunner()

    # Test without any symbols or all-symbols flag
    result = runner.invoke(
        cli,
        ["backfill", "--start-date", "2024-01-01", "--end-date", "2024-01-31"]
    )

    assert result.exit_code != 0
    assert "Either --symbols or --all-symbols must be specified" in result.output


def test_cli_backfill_requires_start_date():
    """Test that backfill command requires --start-date."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["backfill", "--symbols", "BTCUSDT"]
    )

    assert result.exit_code != 0
    assert "--start-date" in result.output or "Missing option" in result.output


def test_cli_backfill_with_symbols():
    """Test backfill command with valid symbols."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "backfill",
            "--symbols", "BTCUSDT",
            "--symbols", "ETHUSDT",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31"
        ]
    )

    assert result.exit_code == 0
    assert "Starting backfill" in result.output
    assert "BTCUSDT" in result.output
    assert "ETHUSDT" in result.output


def test_cli_backfill_with_all_symbols():
    """Test backfill command with --all-symbols flag."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "backfill",
            "--all-symbols",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31"
        ]
    )

    assert result.exit_code == 0
    assert "Starting backfill" in result.output
    assert "ALL perpetual futures" in result.output


def test_cli_backfill_conflicting_options():
    """Test that backfill rejects both --symbols and --all-symbols."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "backfill",
            "--symbols", "BTCUSDT",
            "--all-symbols",
            "--start-date", "2024-01-01"
        ]
    )

    assert result.exit_code != 0
    assert "Cannot specify both --symbols and --all-symbols" in result.output


def test_cli_ingest_daily_default_date():
    """Test ingest-daily command uses default date (yesterday)."""
    runner = CliRunner()

    result = runner.invoke(cli, ["ingest-daily"])

    assert result.exit_code == 0
    assert "Ingesting daily data" in result.output
    # Should mention yesterday's date or similar


def test_cli_ingest_daily_with_date():
    """Test ingest-daily command with specific date."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["ingest-daily", "--date", "2024-01-15"]
    )

    assert result.exit_code == 0
    assert "2024-01-15" in result.output


def test_cli_check_missing_requires_symbols():
    """Test that check-missing command requires --symbols."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["check-missing", "--start", "2024-01-01", "--end", "2024-01-31"]
    )

    assert result.exit_code != 0
    assert "At least one --symbols option must be specified" in result.output


def test_cli_check_missing_with_valid_args():
    """Test check-missing command with valid arguments."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "check-missing",
            "--symbols", "BTCUSDT",
            "--start", "2024-01-01",
            "--end", "2024-01-31"
        ]
    )

    assert result.exit_code == 0
    assert "Checking for missing data" in result.output
    assert "BTCUSDT" in result.output


def test_cli_config_options():
    """Test that CLI config options are properly processed."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--clickhouse-host", "testhost",
            "--clickhouse-port", "9000",
            "--clickhouse-db", "testdb",
            "--max-workers", "8",
            "ingest-daily"
        ]
    )

    assert result.exit_code == 0
    assert "testhost" in result.output
    assert "9000" in result.output
    assert "testdb" in result.output
    assert "8" in result.output


def test_cli_backfill_with_workers_override():
    """Test backfill --workers option overrides config."""
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "--max-workers", "4",
            "backfill",
            "--symbols", "BTCUSDT",
            "--start-date", "2024-01-01",
            "--workers", "16"
        ]
    )

    assert result.exit_code == 0
    assert "16 workers" in result.output
