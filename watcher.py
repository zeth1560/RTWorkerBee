"""
Watchdog-based folder monitoring for new/modified video files.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from queue import Empty, Queue
from typing import Callable

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from config import Settings
from processor import is_video_file

logger = logging.getLogger(__name__)


class ClipFileHandler(FileSystemEventHandler):
    """
    Enqueue candidate clip paths from create/modify/move events (non-recursive watch root).
    """

    def __init__(
        self,
        settings: Settings,
        submit: Callable[[Path], None],
    ) -> None:
        super().__init__()
        self._settings = settings
        self._submit = submit

    def _maybe_submit(self, path: Path) -> None:
        if not path.exists() or path.is_dir():
            return
        if not is_video_file(path, self._settings):
            return
        self._submit(path)

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._maybe_submit(Path(event.src_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._maybe_submit(Path(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        dest = getattr(event, "dest_path", None)
        if dest:
            self._maybe_submit(Path(dest))


class ClipJobQueue:
    """
    Deduplicate and serialize clip paths for a single-worker consumer.

    Tracking is in-memory only (no JSON state file); processed state is reflected by moving
    files out of the watch folder.
    """

    def __init__(self) -> None:
        self._queue: Queue[Path] = Queue()
        self._lock = threading.Lock()
        self._pending: set[str] = set()

    def submit(self, path: Path) -> None:
        key = str(path.resolve(strict=False))
        with self._lock:
            if key in self._pending:
                logger.debug("Already queued", extra={"structured": {"path": key}})
                return
            self._pending.add(key)
        self._queue.put(path)
        logger.info("Queued clip", extra={"structured": {"path": key}})

    def mark_done(self, path: Path) -> None:
        key = str(path.resolve(strict=False))
        with self._lock:
            self._pending.discard(key)

    def get(self, timeout: float) -> Path | None:
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None

    def task_done(self) -> None:
        self._queue.task_done()


def scan_existing_clips(settings: Settings, submit: Callable[[Path], None]) -> None:
    """Enqueue any existing videos in the clips folder on startup."""
    if not settings.clips_folder.is_dir():
        return
    for entry in sorted(settings.clips_folder.iterdir()):
        if entry.is_file() and is_video_file(entry, settings):
            submit(entry)


def start_observer(settings: Settings, event_handler: FileSystemEventHandler) -> Observer:
    settings.clips_folder.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(event_handler, str(settings.clips_folder), recursive=False)
    observer.start()
    logger.info(
        "Watchdog observer started",
        extra={"structured": {"path": str(settings.clips_folder)}},
    )
    return observer
