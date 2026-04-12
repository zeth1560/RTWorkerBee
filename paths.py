"""
Normalized filesystem paths for SQLite storage and recovery comparisons (Windows-friendly).
"""

from __future__ import annotations

from pathlib import Path


def normalize_storage_path(path: Path | str) -> str:
    """
    Canonical string for storing and comparing paths: ``Path.resolve(strict=False)`` as ``str``.

    Use for ``processing_path``, ``incoming_path``, and exact-match SQLite lookups.
    """
    return str(Path(path).resolve(strict=False))
