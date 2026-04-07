"""
Structured logging: human-readable console + JSON lines on disk.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


class JsonLineFormatter(logging.Formatter):
    """One JSON object per log line for file sinks."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        # Standard structured extras
        extras = getattr(record, "structured", None)
        if isinstance(extras, Mapping):
            payload["extra"] = dict(extras)
        return json.dumps(payload, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """Readable console lines; includes structured ``extra`` when present."""

    default_time_format = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, timezone.utc).strftime(self.default_time_format)
        base = f"{ts} | {record.levelname:5} | {record.name} | {record.getMessage()}"
        extras = getattr(record, "structured", None)
        if isinstance(extras, Mapping) and extras:
            try:
                base += " | " + json.dumps(dict(extras), ensure_ascii=False)
            except (TypeError, ValueError):
                base += f" | {extras!r}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def setup_logging(log_dir: Path, log_filename: str = "replaytrove_worker.log") -> None:
    """
    Configure the root logger: INFO to stderr and JSON lines under ``log_dir``.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_filename

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Avoid duplicate handlers if setup_logging is called twice (e.g. tests)
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.INFO)
    console.setFormatter(ConsoleFormatter())

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(JsonLineFormatter())

    root.addHandler(console)
    root.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_extra(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    """Log with structured fields attached as ``record.structured``."""
    logger.log(level, msg, extra={"structured": fields})
