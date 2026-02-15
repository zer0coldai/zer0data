#!/usr/bin/env python3
"""Local data sync: rsync from remote + ingest into ClickHouse.

Usage:
    python sync.py                        # sync + ingest
    python sync.py --no-ingest            # sync only (for initial bulk transfer)
    python sync.py --dry-run              # preview what rsync would transfer
    python sync.py --bwlimit 10000        # bandwidth limit in KB/s
    python sync.py --config path.yaml     # custom config file
"""

from __future__ import annotations

import argparse
import fcntl
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project packages are importable when running outside Docker
# (i.e. ``python sync/sync.py`` from the project root).
# Inside Docker, PYTHONPATH is set by the Dockerfile.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent  # sync -> project root
for _candidate in [
    _PROJECT_ROOT / "ingestor" / "src",
    _SCRIPT_DIR,  # for config / state modules
]:
    if _candidate.is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

from config import OpsConfig  # noqa: E402
from state import SyncState  # noqa: E402

logger = logging.getLogger("zer0data.sync")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(log_dir: Path) -> None:
    """Configure logging to file + stderr."""
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"sync_{today}.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stderr_handler)


# ---------------------------------------------------------------------------
# Rsync
# ---------------------------------------------------------------------------

def run_rsync(
    remote_host: str,
    remote_dir: str,
    local_dir: str,
    *,
    dry_run: bool = False,
    bwlimit: int | None = None,
) -> None:
    """Run rsync to sync data from remote to local.

    Raises ``subprocess.CalledProcessError`` on non-zero exit code.
    """
    cmd: list[str] = [
        "rsync",
        "-az",
        "--partial",
        "--append-verify",
        "--progress",
        "--human-readable",
    ]
    if dry_run:
        cmd.append("--dry-run")
    if bwlimit is not None:
        cmd.append(f"--bwlimit={bwlimit}")

    src = f"{remote_host}:{remote_dir}/"
    dst = f"{local_dir}/"
    cmd.extend([src, dst])

    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode not in (0, 23, 24):
        # 23 = partial transfer due to error, 24 = vanished source files
        # Both are acceptable in incremental sync scenarios
        raise subprocess.CalledProcessError(result.returncode, cmd)
    if result.returncode != 0:
        logger.warning("rsync exited with code %d (non-fatal)", result.returncode)


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def run_ingest(cfg: OpsConfig, state: SyncState) -> None:
    """Process pending SUCCESS markers and ingest into ClickHouse."""
    from zer0data_ingestor.config import ClickHouseConfig as IngestorCHConfig
    from zer0data_ingestor.config import IngestorConfig
    from zer0data_ingestor.ingestor import KlineIngestor

    pending = state.pending_markers()
    if not pending:
        logger.info("No pending markers to ingest")
        return

    logger.info("Found %d pending marker(s) to ingest", len(pending))

    ingestor_config = IngestorConfig(
        clickhouse=IngestorCHConfig(
            host=cfg.clickhouse.host,
            port=cfg.clickhouse.port,
            database=cfg.clickhouse.database,
            username=cfg.clickhouse.username if cfg.clickhouse.username != "default" else None,
            password=cfg.clickhouse.password if cfg.clickhouse.password else None,
        ),
    )

    with KlineIngestor(config=ingestor_config) as ingestor:
        for marker in pending:
            logger.info(
                "Ingesting marker=%s  pattern=%s",
                marker.name,
                marker.glob_pattern,
            )
            try:
                stats = ingestor.ingest_from_directory(
                    source=cfg.local.data_dir,
                    pattern=marker.glob_pattern,
                )
                logger.info(
                    "Marker %s done: %d records written, %d duplicates removed, "
                    "%d gaps filled, %d invalid removed",
                    marker.name,
                    stats.records_written,
                    stats.duplicates_removed,
                    stats.gaps_filled,
                    stats.invalid_records_removed,
                )
                if stats.errors:
                    for err in stats.errors:
                        logger.error("  ingest error: %s", err)

                state.mark_ingested(marker.name)
            except Exception:
                logger.exception("Failed to ingest marker %s", marker.name)
                # Continue with remaining markers rather than aborting
                continue


# ---------------------------------------------------------------------------
# File lock
# ---------------------------------------------------------------------------

class _FileLock:
    """Simple file-based lock using ``fcntl.flock``."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        """Try to acquire the lock. Returns True on success."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self.path, "w")  # noqa: SIM115
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self._fd.close()
            self._fd = None
            return False

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None

    def __enter__(self) -> _FileLock:
        if not self.acquire():
            raise RuntimeError(f"Could not acquire lock: {self.path}")
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync remote data to local and optionally ingest into ClickHouse.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config.yaml (default: auto-detect)",
    )
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        help="Only rsync, skip ingestion (useful for initial bulk sync)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what rsync would transfer without actually syncing",
    )
    parser.add_argument(
        "--bwlimit",
        type=int,
        default=None,
        help="Bandwidth limit for rsync in KB/s",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = OpsConfig.load(args.config)

    _setup_logging(Path(cfg.local.log_dir))

    lock_path = Path(cfg.local.state_dir) / ".sync.lock"
    lock = _FileLock(lock_path)
    if not lock.acquire():
        logger.error("Another sync process is already running (lock: %s)", lock_path)
        return 1

    try:
        state = SyncState(
            data_dir=cfg.local.data_dir,
            state_dir=cfg.local.state_dir,
        )
        state.ensure_dirs()

        # -- rsync ----------------------------------------------------------
        logger.info(
            "Starting rsync: %s:%s -> %s",
            cfg.remote.host,
            cfg.remote.data_dir,
            cfg.local.data_dir,
        )
        run_rsync(
            remote_host=cfg.remote.host,
            remote_dir=cfg.remote.data_dir,
            local_dir=cfg.local.data_dir,
            dry_run=args.dry_run,
            bwlimit=args.bwlimit,
        )
        logger.info("Rsync completed")

        if args.dry_run:
            logger.info("Dry-run mode, skipping ingest")
            return 0

        # -- ingest ---------------------------------------------------------
        if args.no_ingest:
            logger.info("--no-ingest mode, skipping ingestion")
            return 0

        run_ingest(cfg, state)
        logger.info("Sync completed successfully")
        return 0

    except subprocess.CalledProcessError as exc:
        logger.error("rsync failed with exit code %d", exc.returncode)
        return exc.returncode
    except Exception:
        logger.exception("Sync failed with unexpected error")
        return 1
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
