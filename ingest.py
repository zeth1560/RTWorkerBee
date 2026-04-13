"""
Ingest OBS outputs that are not written directly as timestamped clips:

- Instant replay: copy INSTANTREPLAY.mp4 into clips with a local-timestamp name.
  Either poll the source for changes, or react to INSTANT_REPLAY_TRIGGER_FILE (mtime).
- Long recordings: promote from long_clips after stability; optional LONG_CLIPS_TRIGGER_FILE
  wakes a scan early; LONG_CLIPS_SCAN_INTERVAL_SECONDS=0 disables periodic polling.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from config import Settings
from processor import (
    FileLockedError,
    FileStillChangingError,
    is_copying_temp_clip,
    is_file_locked,
    is_video_file,
    move_with_retries,
    should_ignore_file,
    unique_destination,
    wait_until_stable_with_timing,
)

logger = logging.getLogger(__name__)


def _interruptible_sleep(stop: threading.Event, seconds: float) -> bool:
    """
    Sleep in short slices. Returns True if ``stop`` was set during the wait
    (caller should abort the current operation).
    """
    if seconds <= 0:
        return stop.is_set()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if stop.wait(min(0.25, max(0.0, remaining))):
            return True
    return False


def _local_timestamp_basename(settings: Settings, suffix: str) -> str:
    suffix = suffix if suffix.startswith(".") else f".{suffix}"
    zone = ZoneInfo(settings.local_timezone)
    return datetime.now(zone).strftime("%Y-%m-%dT%H-%M-%S") + suffix.lower()


def incoming_clip_local_basename(settings: Settings, suffix: str = ".mp4") -> str:
    """
    Local OBS-style basename for a clip placed in incoming (``YYYY-MM-DDTHH-MM-SS.mp4``).
    Shared with replay-buffer promotion; long-clips ingest continues to use internal helpers only.
    """
    return _local_timestamp_basename(settings, suffix)


def _file_sig(path: Path) -> tuple[int, float] | None:
    try:
        st = path.stat()
        return (st.st_size, st.st_mtime)
    except OSError:
        return None


@dataclass
class _LongClipState:
    sig: tuple[int, float]
    stable_since_mono: float | None


def _instant_replay_try_promote(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
    last_promoted: list[tuple[int, float] | None],
    *,
    skip_if_unchanged_before_wait: bool,
) -> None:
    """
    Stabilize instant replay source, copy to clips via .copying temp, submit final path.
    ``last_promoted`` is a single-element list used as mutable holder.
    """
    path = settings.instant_replay_source
    if path is None:
        return
    path = path.resolve(strict=False)

    if not path.exists() or not path.is_file():
        return

    sig = _file_sig(path)
    if sig is None:
        return

    if (
        skip_if_unchanged_before_wait
        and last_promoted[0] is not None
        and sig == last_promoted[0]
    ):
        return

    logger.info(
        "Instant replay: stabilization starting",
        extra={"structured": {"source": str(path)}},
    )
    try:
        wait_until_stable_with_timing(
            path,
            delay=settings.instant_replay_source_check_seconds,
            retries=settings.instant_replay_source_retries,
            min_age=settings.instant_replay_source_min_age_seconds,
            stable_rounds_required=2,
            log_context="instant_replay_source",
        )
    except FileNotFoundError:
        return
    except FileLockedError:
        logger.debug(
            "Instant replay locked during stabilization; retrying later",
            extra={"structured": {"path": str(path)}},
        )
        return
    except FileStillChangingError:
        logger.debug(
            "Instant replay did not stabilize in time; retrying later",
            extra={"structured": {"path": str(path)}},
        )
        return

    if not path.exists() or not path.is_file():
        return

    sig2 = _file_sig(path)
    if sig2 is None:
        return

    if last_promoted[0] is not None and sig2 == last_promoted[0]:
        return

    if not is_video_file(path, settings):
        last_promoted[0] = sig2
        return

    suffix = path.suffix.lower() if path.suffix else ".mp4"
    if suffix not in settings.video_extensions:
        last_promoted[0] = sig2
        return

    name = _local_timestamp_basename(settings, suffix)
    final_dest = unique_destination(settings.clips_incoming_folder, name)
    temp_dest = final_dest.with_name(final_dest.stem + ".copying" + final_dest.suffix)

    logger.info(
        "Instant replay: starting temp copy",
        extra={
            "structured": {
                "source": str(path),
                "temp": str(temp_dest),
                "final": str(final_dest),
            }
        },
    )
    try:
        shutil.copy2(path, temp_dest)
    except OSError:
        logger.exception(
            "Could not copy instant replay to temp file",
            extra={"structured": {"from": str(path), "temp": str(temp_dest)}},
        )
        return

    try:
        temp_sz = temp_dest.stat().st_size
    except OSError:
        temp_sz = -1
    logger.info(
        "Instant replay: temp copy complete",
        extra={"structured": {"temp": str(temp_dest), "size_bytes": temp_sz}},
    )

    delay = settings.instant_replay_post_copy_delay_seconds
    if delay > 0:
        logger.info(
            "Instant replay: post-copy cooldown started",
            extra={"structured": {"seconds": delay, "temp": str(temp_dest)}},
        )
        if _interruptible_sleep(stop, delay):
            logger.info(
                "Instant replay: aborted during post-copy cooldown (shutdown)",
                extra={"structured": {"temp": str(temp_dest)}},
            )
            try:
                temp_dest.unlink(missing_ok=True)
            except OSError:
                pass
            return
        logger.info(
            "Instant replay: post-copy cooldown finished",
            extra={"structured": {"temp": str(temp_dest)}},
        )

    try:
        temp_dest.replace(final_dest)
    except OSError:
        logger.exception(
            "Could not rename instant replay temp to final",
            extra={"structured": {"temp": str(temp_dest), "final": str(final_dest)}},
        )
        try:
            temp_dest.unlink(missing_ok=True)
        except OSError:
            pass
        return

    logger.info(
        "Instant replay: promoted temp to final",
        extra={"structured": {"from": str(temp_dest), "to": str(final_dest)}},
    )
    last_promoted[0] = sig2
    final_r = final_dest.resolve(strict=False)
    if is_copying_temp_clip(final_r, settings):
        return
    submit(final_r)


def instant_replay_ingest_loop(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
) -> None:
    """Poll instant replay source for signature changes (no trigger file)."""
    path = settings.instant_replay_source
    if path is None:
        return

    path = path.resolve(strict=False)
    last_promoted: list[tuple[int, float] | None] = [None]
    sig0 = _file_sig(path)
    if sig0 is not None:
        last_promoted[0] = sig0

    logger.info(
        "Instant replay ingest started (poll mode)",
        extra={
            "structured": {
                "source": str(path),
                "baseline_sig": last_promoted[0],
            }
        },
    )

    while not stop.wait(0.5):
        if not path.exists() or not path.is_file():
            last_promoted[0] = None
            continue

        sig = _file_sig(path)
        if sig is None:
            continue

        if last_promoted[0] is not None and sig == last_promoted[0]:
            continue

        _instant_replay_try_promote(
            settings,
            submit,
            stop,
            last_promoted,
            skip_if_unchanged_before_wait=True,
        )


def instant_replay_trigger_loop(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
) -> None:
    """
    React to ``INSTANT_REPLAY_TRIGGER_FILE`` mtime updates (Stream Deck / script touches file).
    Does not poll the replay video continuously.
    """
    src = settings.instant_replay_source
    trig = settings.instant_replay_trigger_file
    if src is None or trig is None:
        return

    src = src.resolve(strict=False)
    trig = trig.resolve(strict=False)
    trig.parent.mkdir(parents=True, exist_ok=True)
    if not trig.exists():
        trig.touch()

    last_mtime = trig.stat().st_mtime
    last_promoted: list[tuple[int, float] | None] = [None]
    sig0 = _file_sig(src)
    if sig0 is not None:
        last_promoted[0] = sig0

    logger.info(
        "Instant replay ingest started (trigger file mode)",
        extra={
            "structured": {
                "source": str(src),
                "trigger_file": str(trig),
                "settle_seconds": settings.instant_replay_trigger_settle_seconds,
            }
        },
    )

    while not stop.wait(0.35):
        try:
            m = trig.stat().st_mtime
        except OSError:
            continue

        if m == last_mtime:
            continue

        last_mtime = m
        logger.info(
            "Instant replay: trigger file updated",
            extra={"structured": {"trigger": str(trig)}},
        )

        settle = settings.instant_replay_trigger_settle_seconds
        if settle > 0:
            if _interruptible_sleep(stop, settle):
                continue

        _instant_replay_try_promote(
            settings,
            submit,
            stop,
            last_promoted,
            skip_if_unchanged_before_wait=False,
        )


def _long_clips_scan_pass(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
    states: dict[str, _LongClipState],
    folder: Path,
    stable_need: float,
    *,
    reason: str,
) -> None:
    try:
        entries = list(folder.iterdir())
    except OSError:
        logger.exception(
            "Could not scan long clips folder",
            extra={"structured": {"folder": str(folder)}},
        )
        return

    seen_keys: set[str] = set()

    for entry in entries:
        if not entry.is_file():
            continue
        if should_ignore_file(entry, settings):
            continue
        if is_copying_temp_clip(entry, settings):
            continue
        if not is_video_file(entry, settings):
            continue

        rp = settings.replay_buffer_filename_prefix
        if rp and entry.name.lower().startswith(rp.lower()):
            continue

        key = str(entry.resolve(strict=False)).lower()
        seen_keys.add(key)

        sig = _file_sig(entry)
        if sig is None:
            continue

        prev = states.get(key)
        if prev is None:
            states[key] = _LongClipState(sig=sig, stable_since_mono=None)
            continue

        if prev.sig != sig:
            states[key] = _LongClipState(sig=sig, stable_since_mono=None)
            continue

        now_m = time.monotonic()
        if prev.stable_since_mono is None:
            states[key] = _LongClipState(sig=sig, stable_since_mono=now_m)
            continue

        if (now_m - prev.stable_since_mono) < stable_need:
            continue

        if is_file_locked(entry):
            logger.debug(
                "Long clip eligible by age but still locked; waiting",
                extra={"structured": {"path": str(entry)}},
            )
            continue

        suffix = entry.suffix.lower() if entry.suffix else ".mp4"
        name = _local_timestamp_basename(settings, suffix)
        final_dest = unique_destination(settings.clips_incoming_folder, name)
        temp_dest = final_dest.with_name(final_dest.stem + ".copying" + final_dest.suffix)

        logger.info(
            "Long clip: moving to clips temp file",
            extra={
                "structured": {
                    "from": str(entry),
                    "temp": str(temp_dest),
                    "final": str(final_dest),
                    "wake_reason": reason,
                }
            },
        )
        try:
            move_with_retries(
                entry,
                temp_dest,
                retries=settings.move_retries,
                delay_seconds=settings.move_retry_delay_seconds,
            )
        except Exception:
            logger.exception(
                "Could not move long clip into clips temp",
                extra={"structured": {"from": str(entry), "temp": str(temp_dest)}},
            )
            continue

        delay = settings.instant_replay_post_copy_delay_seconds
        if delay > 0:
            logger.info(
                "Long clip: post-move cooldown before rename",
                extra={"structured": {"seconds": delay, "temp": str(temp_dest)}},
            )
            if _interruptible_sleep(stop, delay):
                logger.warning(
                    "Long clip: shutdown during cooldown; temp left in clips",
                    extra={"structured": {"temp": str(temp_dest)}},
                )
                states.pop(key, None)
                return

        try:
            temp_dest.replace(final_dest)
        except OSError:
            logger.exception(
                "Could not rename long clip temp to final",
                extra={"structured": {"temp": str(temp_dest), "final": str(final_dest)}},
            )
            continue

        states.pop(key, None)
        logger.info(
            "Long clip: moved to clips after extended stability",
            extra={
                "structured": {
                    "from": str(entry),
                    "to": str(final_dest),
                    "stable_seconds": stable_need,
                    "wake_reason": reason,
                }
            },
        )
        final_r = final_dest.resolve(strict=False)
        if is_copying_temp_clip(final_r, settings):
            continue
        submit(final_r)

    stale = [k for k in states if k not in seen_keys]
    for k in stale:
        states.pop(k, None)


def long_clips_ingest_loop(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
) -> None:
    folder = settings.long_clips_folder
    if folder is None:
        return

    folder = folder.resolve(strict=False)
    clips_root = settings.clips_incoming_folder.resolve(strict=False)
    if folder == clips_root:
        logger.error(
            "Long clips folder must not be the same as the incoming clips folder; long ingest disabled",
            extra={
                "structured": {
                    "folder": str(folder),
                    "clips_incoming_folder": str(clips_root),
                }
            },
        )
        return

    folder.mkdir(parents=True, exist_ok=True)

    interval = settings.long_clips_scan_interval_seconds
    trig_path = settings.long_clips_trigger_file
    if trig_path is not None:
        trig_path = trig_path.resolve(strict=False)
        trig_path.parent.mkdir(parents=True, exist_ok=True)
        if not trig_path.exists():
            trig_path.touch()

    if interval <= 0 and trig_path is None:
        logger.warning(
            "Long clips: LONG_CLIPS_SCAN_INTERVAL_SECONDS is 0 and no LONG_CLIPS_TRIGGER_FILE; "
            "long clips ingest will not run",
            extra={"structured": {"folder": str(folder)}},
        )
        return

    stable_need = settings.long_clip_stable_seconds
    states: dict[str, _LongClipState] = {}
    last_trig_mtime: float = 0.0
    if trig_path is not None:
        try:
            last_trig_mtime = trig_path.stat().st_mtime
        except OSError:
            last_trig_mtime = 0.0

    logger.info(
        "Long clips ingest started",
        extra={
            "structured": {
                "folder": str(folder),
                "stable_seconds": stable_need,
                "scan_interval_seconds": interval,
                "trigger_file": str(trig_path) if trig_path else None,
            }
        },
    )

    while not stop.is_set():
        reason = "periodic"

        if interval > 0:
            deadline = time.monotonic() + interval
            triggered = False
            while time.monotonic() < deadline:
                if stop.wait(0.2):
                    return
                if trig_path is not None:
                    try:
                        tm = trig_path.stat().st_mtime
                        if tm != last_trig_mtime:
                            last_trig_mtime = tm
                            triggered = True
                            reason = "trigger_file"
                            logger.info(
                                "Long clips: trigger file updated",
                                extra={"structured": {"trigger": str(trig_path)}},
                            )
                            break
                    except OSError:
                        pass
            if not triggered:
                reason = "periodic"
        else:
            while not stop.wait(0.35):
                if trig_path is None:
                    return
                try:
                    tm = trig_path.stat().st_mtime
                    if tm != last_trig_mtime:
                        last_trig_mtime = tm
                        reason = "trigger_file"
                        logger.info(
                            "Long clips: trigger file updated",
                            extra={"structured": {"trigger": str(trig_path)}},
                        )
                        break
                except OSError:
                    pass
            if stop.is_set():
                return

        _long_clips_scan_pass(
            settings,
            submit,
            stop,
            states,
            folder,
            stable_need,
            reason=reason,
        )
