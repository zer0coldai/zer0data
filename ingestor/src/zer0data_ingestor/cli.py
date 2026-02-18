"""CLI interface for zer0data ingestor."""

import logging
from types import SimpleNamespace

import click

from zer0data_ingestor.config import IngestorConfig, ClickHouseConfig
from zer0data_ingestor.fetcher.sources.coinmetrics import run as run_coinmetrics
from zer0data_ingestor.fetcher.sources.exchange_info import run as run_exchange_info
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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
    default="**/*.zip",
    help="File pattern to match (default: **/*.zip)",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force re-import of data even if it already exists",
)
@click.pass_context
def ingest_from_dir(
    ctx: click.Context,
    source: str,
    symbols: tuple,
    pattern: str,
    force: bool,
) -> None:
    """Ingest kline data from a directory of downloaded zip files.

    Download data first using binance-public-data scripts, then ingest with this command.
    Interval is automatically extracted from each filename (e.g. BTCUSDT-1h-2024-01-01.zip).

    By default, files with existing data are skipped (incremental import).
    Use --force to re-import all data.

    Examples:

        # Ingest all files from directory (skip existing data)
        zer0data-ingestor ingest-from-dir --source ./data/download

        # Force re-import all data
        zer0data-ingestor ingest-from-dir --source ./data/download --force

        # Ingest specific symbols only
        zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --symbols ETHUSDT
    """
    config = ctx.obj["config"]

    symbols_list = list(symbols) if symbols else None

    click.echo(f"Ingesting data from: {source}")
    if symbols_list:
        click.echo(f"Symbols: {', '.join(symbols_list)}")
    else:
        click.echo("Symbols: ALL")
    click.echo(f"Pattern: {pattern}")
    click.echo(f"Mode: {'FORCE (re-import all)' if force else 'INCREMENTAL (skip existing)'}")
    click.echo(
        f"ClickHouse: {config.clickhouse.host}:{config.clickhouse.port}"
        f"/{config.clickhouse.database}"
    )

    try:
        with KlineIngestor(config=config) as ingestor:
            stats = ingestor.ingest_from_directory(
                source=source,
                symbols=symbols_list,
                pattern=pattern,
                force=force,
            )

        click.echo("\nIngestion completed:")
        click.echo(f"  Files processed: {stats.files_processed}")
        click.echo(f"  Records written: {stats.records_written}")
        click.echo(f"  Duplicates removed: {stats.duplicates_removed}")
        click.echo(f"  Gaps filled: {stats.gaps_filled}")
        click.echo(f"  Invalid records removed: {stats.invalid_records_removed}")

        if stats.errors:
            click.echo(f"  Errors: {len(stats.errors)}")
            for error in stats.errors:
                click.echo(f"    - {error}")

    except Exception as e:
        click.echo(f"\nError during ingestion: {e}", err=True)
        raise click.ClickException(f"Ingestion failed: {e}")


@cli.group()
def ingest_source() -> None:
    """Ingest data from external source providers."""


def _fetcher_base_args(ctx: click.Context) -> dict:
    config = ctx.obj["config"]
    return {
        "clickhouse_host": config.clickhouse.host,
        "clickhouse_port": config.clickhouse.port,
        "clickhouse_db": config.clickhouse.database,
        "clickhouse_user": config.clickhouse.username or "default",
        "clickhouse_password": config.clickhouse.password or "",
    }


@ingest_source.command("exchange-info")
@click.option(
    "--markets",
    multiple=True,
    default=("um",),
    type=click.Choice(["spot", "um", "cm"]),
    help="Markets to fetch (default: um). Can be repeated.",
)
@click.option("--timeout", type=int, default=20, show_default=True)
@click.option("--retries", type=int, default=3, show_default=True)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_context
def ingest_source_exchange_info(
    ctx: click.Context,
    markets: tuple[str, ...],
    timeout: int,
    retries: int,
    dry_run: bool,
) -> None:
    """Fetch Binance exchangeInfo and ingest raw payloads."""
    try:
        args = SimpleNamespace(
            **_fetcher_base_args(ctx),
            markets=list(markets),
            timeout=timeout,
            retries=retries,
            dry_run=dry_run,
            log_level="INFO",
        )
        result = run_exchange_info(args)
        click.echo(
            f"exchange-info done: files_total={result.files_total} "
            f"files_ok={result.files_ok} rows_written={result.rows_written} errors={result.errors}"
        )
    except Exception as exc:
        raise click.ClickException(f"Source ingestion failed: {exc}") from exc


@ingest_source.command("coinmetrics")
@click.option(
    "--symbols",
    multiple=True,
    help="Optional symbols to fetch (e.g. --symbols btc --symbols eth).",
)
@click.option("--timeout", type=int, default=30, show_default=True)
@click.option("--retries", type=int, default=3, show_default=True)
@click.option("--head", type=int, default=3, show_default=True)
@click.option("--tail", type=int, default=3, show_default=True)
@click.option("--batch-size", type=int, default=100000, show_default=True)
@click.option("--max-partitions-per-insert-block", type=int, default=1000, show_default=True)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_context
def ingest_source_coinmetrics(
    ctx: click.Context,
    symbols: tuple[str, ...],
    timeout: int,
    retries: int,
    head: int,
    tail: int,
    batch_size: int,
    max_partitions_per_insert_block: int,
    dry_run: bool,
) -> None:
    """Fetch CoinMetrics CSV and ingest factors table."""
    try:
        args = SimpleNamespace(
            **_fetcher_base_args(ctx),
            symbols=list(symbols) if symbols else None,
            timeout=timeout,
            retries=retries,
            head=head,
            tail=tail,
            batch_size=batch_size,
            max_partitions_per_insert_block=max_partitions_per_insert_block,
            dry_run=dry_run,
            log_level="INFO",
        )
        result = run_coinmetrics(args)
        click.echo(
            f"coinmetrics done: files_total={result.files_total} "
            f"files_ok={result.files_ok} rows_written={result.rows_written} errors={result.errors}"
        )
    except Exception as exc:
        raise click.ClickException(f"Source ingestion failed: {exc}") from exc


if __name__ == "__main__":
    cli()
