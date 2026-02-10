"""CLI interface for zer0data ingestor."""

import click
from datetime import date, timedelta, datetime as dt
from typing import Optional

from zer0data_ingestor.config import IngestorConfig


@click.group()
@click.option(
    "--clickhouse-host",
    envvar="CLICKHOUSE_HOST",
    default="localhost",
    help="ClickHouse server host",
)
@click.option(
    "--clickhouse-port",
    envvar="CLICKHOUSE_PORT",
    default=8123,
    type=int,
    help="ClickHouse HTTP port",
)
@click.option(
    "--clickhouse-db",
    envvar="CLICKHOUSE_DB",
    default="zer0data",
    help="ClickHouse database name",
)
@click.option(
    "--clickhouse-user",
    envvar="CLICKHOUSE_USER",
    default="default",
    help="ClickHouse username",
)
@click.option(
    "--clickhouse-password",
    envvar="CLICKHOUSE_PASSWORD",
    default="",
    help="ClickHouse password",
)
@click.option(
    "--data-dir",
    envvar="DATA_DIR",
    default="./data/download",
    help="Data download directory",
)
@click.option(
    "--max-workers",
    envvar="MAX_WORKERS",
    default=4,
    type=int,
    help="Maximum number of worker threads",
)
@click.pass_context
def cli(
    ctx: click.Context,
    clickhouse_host: str,
    clickhouse_port: int,
    clickhouse_db: str,
    clickhouse_user: str,
    clickhouse_password: str,
    data_dir: str,
    max_workers: int,
) -> None:
    """Zer0data Ingestor - Binance perpetual futures data ingestion tool.

    This CLI provides commands for backfilling historical data and ingesting daily
    kline (candlestick) data from Binance perpetual futures markets.
    """
    ctx.ensure_object(dict)

    # Store configuration in context for subcommands
    ctx.obj["config"] = {
        "clickhouse_host": clickhouse_host,
        "clickhouse_port": clickhouse_port,
        "clickhouse_db": clickhouse_db,
        "clickhouse_user": clickhouse_user,
        "clickhouse_password": clickhouse_password,
        "data_dir": data_dir,
        "max_workers": max_workers,
    }


@cli.command()
@click.option(
    "--symbols",
    "-s",
    multiple=True,
    help="Specific trading symbols to backfill (e.g., BTCUSDT). Can be specified multiple times.",
)
@click.option(
    "--all-symbols",
    is_flag=True,
    help="Backfill all available perpetual futures symbols",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date for backfill (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="End date for backfill (YYYY-MM-DD). Defaults to yesterday.",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    default=None,
    help="Number of worker threads for parallel download. Overrides --max-workers.",
)
@click.pass_context
def backfill(
    ctx: click.Context,
    symbols: tuple,
    all_symbols: bool,
    start_date: date,
    end_date: Optional[date],
    workers: Optional[int],
) -> None:
    """Backfill historical kline data for specified symbols and date range.

    Downloads and inserts historical kline data from Binance perpetual futures
    into ClickHouse for the specified date range.

    Examples:

        # Backfill BTCUSDT and ETHUSDT for January 2024
        zer0data-ingestor backfill --symbols BTCUSDT --symbols ETHUSDT --start-date 2024-01-01 --end-date 2024-01-31

        # Backfill all symbols for a specific date range with 8 workers
        zer0data-ingestor backfill --all-symbols --start-date 2024-01-01 --end-date 2024-01-31 --workers 8
    """
    config = ctx.obj["config"]

    # Use provided workers or fall back to config
    num_workers = workers if workers is not None else config["max_workers"]

    # Default end_date to yesterday if not specified
    if end_date is None:
        end_date = date.today() - timedelta(days=1)

    # Validate arguments
    if not symbols and not all_symbols:
        raise click.UsageError(
            "Either --symbols or --all-symbols must be specified"
        )

    if symbols and all_symbols:
        raise click.UsageError(
            "Cannot specify both --symbols and --all-symbols"
        )

    click.echo(f"Starting backfill with {num_workers} workers...")

    # Convert datetime objects to date objects for consistent formatting
    start_date_obj = start_date.date() if hasattr(start_date, 'date') else start_date
    end_date_obj = end_date.date() if hasattr(end_date, 'date') else end_date

    click.echo(f"Date range: {start_date_obj} to {end_date_obj}")

    if all_symbols:
        click.echo("Symbols: ALL perpetual futures")
    else:
        click.echo(f"Symbols: {', '.join(symbols)}")

    click.echo(f"ClickHouse: {config['clickhouse_host']}:{config['clickhouse_port']}/{config['clickhouse_db']}")
    click.echo(f"Data directory: {config['data_dir']}")

    # TODO: Implement actual backfill logic
    # This will use BinanceKlineDownloader and ClickHouseWriter
    click.echo("\nBackfill command executed (placeholder - implementation pending)")


@cli.command()
@click.option(
    "--date",
    "date_str",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Date to ingest (YYYY-MM-DD). Defaults to yesterday.",
)
@click.pass_context
def ingest_daily(
    ctx: click.Context,
    date_str: Optional[dt],
) -> None:
    """Ingest daily kline data.

    Downloads and inserts kline data for a single day. If no date is specified,
    defaults to yesterday. This command is intended for daily scheduled runs
    to keep the database up to date.

    Examples:

        # Ingest data for yesterday (default)
        zer0data-ingestor ingest-daily

        # Ingest data for a specific date
        zer0data-ingestor ingest-daily --date 2024-01-15
    """
    config = ctx.obj["config"]

    # Default to yesterday if not specified
    if date_str is None:
        target_date = date.today() - timedelta(days=1)
    else:
        target_date = date_str.date()

    click.echo(f"Ingesting daily data for: {target_date}")
    click.echo(f"ClickHouse: {config['clickhouse_host']}:{config['clickhouse_port']}/{config['clickhouse_db']}")
    click.echo(f"Data directory: {config['data_dir']}")
    click.echo(f"Workers: {config['max_workers']}")

    # TODO: Implement actual daily ingestion logic
    # This will use BinanceKlineDownloader and ClickHouseWriter
    click.echo("\nDaily ingestion command executed (placeholder - implementation pending)")


@cli.command()
@click.option(
    "--symbols",
    "-s",
    multiple=True,
    help="Specific trading symbols to check (e.g., BTCUSDT). Can be specified multiple times.",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date for check (YYYY-MM-DD)",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="End date for check (YYYY-MM-DD)",
)
@click.pass_context
def check_missing(
    ctx: click.Context,
    symbols: tuple,
    start: date,
    end: date,
) -> None:
    """Check for missing kline data in the database.

    Queries ClickHouse to identify gaps in kline data for the specified
    symbols and date range. Useful for verifying data completeness and
    identifying missing data that needs to be backfilled.

    Examples:

        # Check for missing BTCUSDT data in January 2024
        zer0data-ingestor check-missing --symbols BTCUSDT --start 2024-01-01 --end 2024-01-31

        # Check multiple symbols
        zer0data-ingestor check-missing --symbols BTCUSDT --symbols ETHUSDT --start 2024-01-01 --end 2024-01-31
    """
    config = ctx.obj["config"]

    if not symbols:
        raise click.UsageError(
            "At least one --symbols option must be specified"
        )

    click.echo(f"Checking for missing data...")
    click.echo(f"Date range: {start.date()} to {end.date()}")
    click.echo(f"Symbols: {', '.join(symbols)}")
    click.echo(f"ClickHouse: {config['clickhouse_host']}:{config['clickhouse_port']}/{config['clickhouse_db']}")

    # TODO: Implement actual missing data check logic
    # This will query ClickHouse to find gaps in the data
    click.echo("\nCheck missing command executed (placeholder - implementation pending)")


if __name__ == "__main__":
    cli()
