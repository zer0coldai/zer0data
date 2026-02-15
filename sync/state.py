"""Sync state tracking via marker files.

The remote downloader creates ``_SUCCESS__{date}__{market}__{interval}``
marker files in the data directory after a successful download batch.

This module provides helpers to:
- Discover pending (not yet ingested) SUCCESS markers
- Record that a marker has been ingested
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

# Matches: _SUCCESS__2026-02-14__um__1h
_MARKER_RE = re.compile(
    r"^_SUCCESS__(?P<date>\d{4}-\d{2}-\d{2})__(?P<market>[a-z]+)__(?P<interval>\w+)$"
)


@dataclass(frozen=True)
class SuccessMarker:
    """A parsed ``_SUCCESS__`` marker."""

    name: str
    date: str
    market: str
    interval: str

    @property
    def glob_pattern(self) -> str:
        """Glob pattern that matches zip files for this marker."""
        return f"**/*-{self.interval}-{self.date}.zip"


class SyncState:
    """Manages ``_SUCCESS`` and ``_ingested`` marker files."""

    def __init__(self, data_dir: Path | str, state_dir: Path | str) -> None:
        self.data_dir = Path(data_dir)
        self.state_dir = Path(state_dir)

    def ensure_dirs(self) -> None:
        """Create data and state directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def pending_markers(self) -> list[SuccessMarker]:
        """Return SUCCESS markers that have not been ingested yet.

        Results are sorted by (date, market, interval) so older data is
        processed first.
        """
        markers: list[SuccessMarker] = []
        for path in self.data_dir.glob("_SUCCESS__*"):
            marker = self._parse_marker(path.name)
            if marker is None:
                continue
            if not self.is_ingested(marker.name):
                markers.append(marker)

        markers.sort(key=lambda m: (m.date, m.market, m.interval))
        return markers

    def is_ingested(self, marker_name: str) -> bool:
        """Check if a marker has already been ingested."""
        return (self.state_dir / marker_name).exists()

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def mark_ingested(self, marker_name: str) -> None:
        """Record that a marker has been successfully ingested."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / marker_name).touch()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_marker(name: str) -> SuccessMarker | None:
        """Parse a marker filename into a ``SuccessMarker`` or ``None``."""
        m = _MARKER_RE.match(name)
        if m is None:
            return None
        return SuccessMarker(
            name=name,
            date=m.group("date"),
            market=m.group("market"),
            interval=m.group("interval"),
        )
