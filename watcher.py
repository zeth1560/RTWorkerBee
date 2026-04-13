"""
Watchdog-based folder monitoring for new/modified video files.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from queue import Empty, Queue
from typing import Callable

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from config import Settings
from processor import (
    clip_path_inflight,
    is_copying_temp_clip,
    is_recently_completed_clip,
    is_replay_buffer_basename,
    is_video_file,
    should_ignore_file,
)

logger = logging.getLogger(__name__)


def _path_key(path: Path) -> str:
    return str(path.resolve(strict=False)).lower()


class ClipFileHandler(FileSystemEventHandler):
    """
    Enqueue candidate clip paths from create/modify/move events (non-recursive watch root).

    This handler also throttles repeat events for the same path because watchdog can fire
    multiple created/modified events while a file is still being written.
    """

    def __init__(
        self,
        settings: Settings,
        submit: Callable[[Path], None],
    ) -> None:
        super().__init__()
        self._settings = settings
        self._submit = submit
        self._lock = threading.Lock()
        self._last_submitted_at: dict[str, float] = {}
        self._throttle_seconds = max(
            0.25,
            min(
                settings.file_stable_check_seconds,
                settings.locked_file_requeue_delay_seconds
                if settings.locked_file_requeue_delay_seconds > 0
                else settings.file_stable_check_seconds,
            ),
        )

    def _should_throttle(self, path: Path) -> bool:
        key = _path_key(path)
        now = time.time()

        with self._lock:
            last = self._last_submitted_at.get(key)
            if last is not None and (now - last) < self._throttle_seconds:
                return True
            self._last_submitted_at[key] = now

            if len(self._last_submitted_at) > 2000:
                cutoff = now - max(self._throttle_seconds * 10, 60.0)
                stale_keys = [k for k, ts in self._last_submitted_at.items() if ts < cutoff]
                for stale_key in stale_keys:
                    self._last_submitted_at.pop(stale_key, None)

            return False

    def _maybe_submit(self, path: Path) -> None:
        if not path.exists() or path.is_dir():
            return

        normalized = path.resolve(strict=False)

        if is_copying_temp_clip(normalized, self._settings):
            logger.debug(
                "Watcher ignoring ingest temp file (.copying)",
                extra={"structured": {"path": str(normalized)}},
            )
            return
        if clip_path_inflight(normalized):
            logger.debug(
                "Watcher skipping path already in-flight",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if is_recently_completed_clip(normalized):
            logger.debug(
                "Watcher ignoring recently completed clip",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if should_ignore_file(normalized, self._settings):
            logger.debug(
                "Ignoring file event by configured name/pattern",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if is_replay_buffer_basename(normalized, self._settings):
            logger.debug(
                "Watcher ignoring replay-buffer basename",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if not is_video_file(normalized, self._settings):
            return

        if normalized.parent.resolve(strict=False) != self._settings.clips_incoming_folder.resolve(
            strict=False
        ):
            return

        if self._should_throttle(normalized):
            logger.debug(
                "Throttled duplicate file event",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        self._submit(normalized)

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

    Tracking is in-memory only; processed state is reflected by moving
    files out of the watch folder.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queue: Queue[Path] = Queue()
        self._lock = threading.Lock()
        self._pending: set[str] = set()

    def submit(self, path: Path) -> None:
        normalized = path.resolve(strict=False)

        if is_copying_temp_clip(normalized, self._settings):
            logger.debug(
                "Queue rejected: ingest temp file (.copying)",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if is_replay_buffer_basename(normalized, self._settings):
            logger.info(
                "Queue rejected: replay-buffer basename (promoted via replay path only)",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        if clip_path_inflight(normalized):
            logger.info(
                "Queue rejected: clip path already in-flight",
                extra={"structured": {"path": str(normalized)}},
            )
            return

        key = _path_key(normalized)

        with self._lock:
            if key in self._pending:
                logger.debug("Already queued", extra={"structured": {"path": str(normalized)}})
                return
            self._pending.add(key)

        self._queue.put(normalized)
        logger.info("Queued clip", extra={"structured": {"path": str(normalized)}})

    def mark_done(self, path: Path) -> None:
        key = _path_key(path)
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
    """Enqueue any existing videos in the incoming clips folder on startup."""
    if not settings.clips_incoming_folder.is_dir():
        return

    for entry in sorted(settings.clips_incoming_folder.iterdir()):
        if not entry.is_file():
            continue
        if is_copying_temp_clip(entry, settings):
            continue
        resolved = entry.resolve(strict=False)
        if clip_path_inflight(resolved):
            continue
        if should_ignore_file(entry, settings):
            continue
        if is_replay_buffer_basename(resolved, settings):
            logger.info(
                "Startup scan: skipping replay-buffer basename in incoming",
                extra={"structured": {"path": str(resolved)}},
            )
            continue
        if is_recently_completed_clip(entry):
            continue
        if is_video_file(entry, settings):
            submit(resolved)


def scan_processing_resume(settings: Settings, submit: Callable[[Path], None]) -> None:
    """Resume incomplete jobs: enqueue videos still under ``clips_processing_folder``."""
    proc = settings.clips_processing_folder
    if not proc.is_dir():
        return
    for entry in sorted(proc.iterdir()):
        if not entry.is_file():
            continue
        if is_copying_temp_clip(entry, settings):
            continue
        resolved = entry.resolve(strict=False)
        if clip_path_inflight(resolved):
            continue
        if should_ignore_file(entry, settings):
            continue
        if is_replay_buffer_basename(resolved, settings):
            logger.info(
                "Processing resume scan: skipping replay-buffer basename",
                extra={"structured": {"path": str(resolved)}},
            )
            continue
        if is_video_file(entry, settings):
            submit(resolved)


def start_observer(settings: Settings, event_handler: FileSystemEventHandler) -> Observer:
    settings.clips_incoming_folder.mkdir(parents=True, exist_ok=True)
    settings.clips_processing_folder.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(event_handler, str(settings.clips_incoming_folder), recursive=False)
    observer.start()
    logger.info(
        "Watchdog observer started",
        extra={"structured": {"path": str(settings.clips_incoming_folder)}},
    )
    return observer