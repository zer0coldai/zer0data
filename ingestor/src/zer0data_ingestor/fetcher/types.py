from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FetchResult:
    files_total: int = 0
    files_ok: int = 0
    rows_written: int = 0
    errors: int = 0
