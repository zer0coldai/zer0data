"""CLI interface for zer0data ingestor."""

import click
from pathlib import Path
from typing import Optional

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.ingestor import KlineIngestor


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
@click.pass_context
def cli(
    ctx: click.Context,
    clickhouse_host: str,
    clickhouse_port: int,
    clickhouse_db: str,
    clickhouse_user: str,
    clickhouse_password: str,
) -> None:
    """Zer0data Ingestor - Binance perpetual futures data ingestion tool.

    This CLI provides commands for ingesting kline (candlestick) data from
    binance-public-data download files into ClickHouse.

    Download data using binance-public-data scripts:
    https://github.com/binance/binance-public-data
    """
    ctx.ensure_object(dict)

    # Store configuration in context for subcommands
    ctx.obj["config"] = IngestorConfig(
        clickhouse=ClickHouseConfig(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_db,
            username=clickhouse_user if clickhouse_user != "default" else None,
            password=clickhouse_password if clickhouse_password else None,
        ),
    )


@cli.command()
@click.option(
    "--source",
    "-s",
    required=True,
    type=click.Path(exists=True),
    help="Directory containing downloaded zip files",
)
@click.option(
    "--symbols",
    multiple=True,
    help="Specific symbols to ingest (e.g., BTCUSDT). Can be specified multiple times.",
)
@click.option(
    "--pattern",
    default="*.zip",
    help="File pattern to match (default: *.zip)",
)
@click.pass_context
def ingest_from_dir(
    ctx: click.Context,
    source: str,
    symbols: tuple,
    pattern: str,
) -> None:
    """Ingest kline data from a directory of downloaded zip files.

    Download data first using binance-public-data scripts, then ingest with this command.

    Examples:

        # Ingest all files from directory
        zer0data-ingestor ingest-from-dir --source ./data/download

        # Ingest specific symbols only
        zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --symbols ETHUSDT
    """
    config = ctx.obj["config"]

    # Convert tuple to list if symbols provided
    symbols_list = list(symbols) if symbols else None

    click.echo(f"Ingesting data from: {source}")
    if symbols_list:
        click.echo(f"Symbols: {', '.join(symbols_list)}")
    else:
        click.echo("Symbols: ALL")
    click.echo(f"Pattern: {pattern}")
    click.echo(f"ClickHouse: {config.clickhouse.host}:{config.clickhouse.port}/{config.clickhouse.database}")

    try:
        # Create ingestor with the provided config
        ingestor = KlineIngestor(config=config)

        # Ingest from directory
        stats = ingestor.ingest_from_directory(
            source=source,
            symbols=symbols_list,
            pattern=pattern
        )

        # Display results
        click.echo(f"\nIngestion completed:")
        click.echo(f"  Records written: {stats.records_written}")
        click.echo(f"  Files processed: {stats.files_processed}")

        if stats.errors:
            click.echo(f"  Errors: {len(stats.errors)}")
            for error in stats.errors:
                click.echo(f"    - {error}")

    except Exception as e:
        click.echo(f"\nError during ingestion: {e}", err=True)
        raise click.ClickException(f"Ingestion failed: {e}")
    finally:
        # Ensure ingestor is properly closed
        if 'ingestor' in locals():
            ingestor.close()


if __name__ == "__main__":
    cli()
