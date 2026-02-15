"""Transfer backends: rclone (R2) and rsync.

rclone is configured entirely via environment variables so that no
config file is needed inside the container.  Docker Compose injects
the ``RCLONE_CONFIG_R2_*`` variables.

Required rclone env vars (set in compose.yml / .env):
    RCLONE_CONFIG_R2_TYPE=s3
    RCLONE_CONFIG_R2_PROVIDER=Cloudflare
    RCLONE_CONFIG_R2_ACCESS_KEY_ID=...
    RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=...
    RCLONE_CONFIG_R2_ENDPOINT=...
    RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
"""

from __future__ import annotations

import logging
import subprocess

from config import OpsConfig

logger = logging.getLogger("zer0data.transfer")


# ---------------------------------------------------------------------------
# R2 helpers
# ---------------------------------------------------------------------------

def _r2_remote_path(cfg: OpsConfig) -> str:
    """Build the rclone remote path, e.g. ``r2:zer0data/download``."""
    prefix = cfg.storage.r2.prefix.strip("/")
    if prefix:
        return f"r2:{cfg.storage.r2.bucket}/{prefix}"
    return f"r2:{cfg.storage.r2.bucket}"


def r2_upload(
    cfg: OpsConfig,
    *,
    dry_run: bool = False,
    cleanup: bool = False,
) -> None:
    """Upload local data directory to R2.

    Runs on the **remote** server after the downloader finishes.

    Args:
        cfg: Ops configuration.
        dry_run: Preview only, don't transfer.
        cleanup: Delete local files after successful upload.
    """
    src = f"{cfg.local.data_dir}/"
    dst = _r2_remote_path(cfg)

    cmd: list[str] = [
        "rclone",
        "sync" if cleanup else "copy",
        src,
        dst,
        f"--transfers={cfg.storage.r2.transfers}",
        "--progress",
        "--stats-one-line",
        "--retries=3",
        "--retries-sleep=10s",
        "--s3-chunk-size=64M",
        "--s3-upload-concurrency=4",
        "--checksum",
    ]
    if dry_run:
        cmd.append("--dry-run")

    logger.info("R2 upload: %s -> %s", src, dst)
    logger.info("Running: %s", " ".join(cmd))
    _run_rclone(cmd)

    if cleanup and not dry_run:
        logger.info("Cleanup: removing local data files")
        # Keep directory structure but remove zip files
        clean_cmd = [
            "rclone", "delete", src,
            "--include", "*.zip",
        ]
        _run_rclone(clean_cmd)


def r2_pull(
    cfg: OpsConfig,
    *,
    dry_run: bool = False,
) -> None:
    """Pull data from R2 to local directory.

    Runs on the **local** server.

    Args:
        cfg: Ops configuration.
        dry_run: Preview only, don't transfer.
    """
    src = _r2_remote_path(cfg)
    dst = f"{cfg.local.data_dir}/"

    cmd: list[str] = [
        "rclone",
        "copy",
        src,
        dst,
        f"--transfers={cfg.storage.r2.transfers}",
        "--progress",
        "--stats-one-line",
    ]
    if dry_run:
        cmd.append("--dry-run")

    logger.info("R2 pull: %s -> %s", src, dst)
    logger.info("Running: %s", " ".join(cmd))
    _run_rclone(cmd)


# ---------------------------------------------------------------------------
# rsync (legacy, kept as fallback)
# ---------------------------------------------------------------------------

def rsync_pull(
    cfg: OpsConfig,
    *,
    dry_run: bool = False,
    bwlimit: int | None = None,
) -> None:
    """Pull data from remote server via rsync over SSH."""
    cmd: list[str] = [
        "rsync",
        "-a",
        "--partial",
        "--append-verify",
        "--progress",
        "--human-readable",
    ]
    if dry_run:
        cmd.append("--dry-run")
    if bwlimit is not None:
        cmd.append(f"--bwlimit={bwlimit}")

    src = f"{cfg.remote.host}:{cfg.remote.data_dir}/"
    dst = f"{cfg.local.data_dir}/"
    cmd.extend([src, dst])

    logger.info("rsync pull: %s -> %s", src, dst)
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode not in (0, 23, 24):
        raise subprocess.CalledProcessError(result.returncode, cmd)
    if result.returncode != 0:
        logger.warning("rsync exited with code %d (non-fatal)", result.returncode)


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _run_rclone(cmd: list[str]) -> None:
    """Run an rclone command, raising on failure."""
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)
