"""
Replay-buffer command path (isolated from long-recording ingest).

Handles OBS ``replay_*.mkv`` files under ``LONG_CLIPS_FOLDER`` only: discover, stabilize,
remux with ffmpeg (stream copy) into a ``*.copying.mp4`` temp under ``clips_incoming``, then copy via a
same-directory temp file and atomic replace into ``INSTANTREPLAY.mkv``, atomically place the remuxed
MP4 in ``clips_incoming`` using the source basename with the replay prefix removed and extension
``.mp4`` (e.g. ``replay_ 2026-04-13T15-06-42.mkv`` → ``2026-04-13T15-06-42.mp4``), verify both, then
remove the long_clips source (or move it to ``FAILED_FOLDER`` if remux never succeeds).

Long-clips ingest skips the same filename prefix so replay-buffer clips are not promoted as
long recordings (see :mod:`ingest`).
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import shutil
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from config import Settings
from ingest import incoming_clip_local_basename
from processor import (
    is_copying_temp_clip,
    is_file_locked,
    move_with_retries,
    remux_to_mp4_with_retries,
    should_ignore_file,
    unique_destination,
)

logger = logging.getLogger(__name__)

# Serialize dual-copy to INSTANTREPLAY + incoming (auto-sync vs CLI/HTTP).
replay_promotion_lock = threading.Lock()

_TRIGGER_NUMERIC = re.compile(r"^\d+(\.\d+)?$")


def _replay_corr_struct(request_id: str | None, data: dict) -> dict:
    """Merge optional HTTP/API ``request_id`` into structured log payloads."""
    if request_id:
        return {**data, "request_id": request_id}
    return data

# Final filename required for scoreboard playback (case-insensitive match on basename).
SCOREBOARD_REPLAY_BASENAME = "INSTANTREPLAY.mkv"


@dataclass
class ProcessLatestReplayResult:
    """Structured outcome for ``process-latest-replay`` (JSON-serializable)."""

    success: bool
    selected_source_path: str | None
    incoming_path: str | None
    detected_at: str | None
    stability_confirmed: bool
    failure_reason: str | None
    scoreboard_replay_path: str | None = None
    processing_error: str | None = None
    source_deleted: bool | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def parse_trigger_timestamp(raw: str) -> float:
    """
    Parse a trigger time as Unix epoch (seconds) or ISO-8601 (e.g. ``2026-04-13T12:00:00Z``).
    """
    s = raw.strip()
    if _TRIGGER_NUMERIC.match(s):
        return float(s)
    normalized = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _replay_matches(path: Path, prefix: str) -> bool:
    if not path.is_file():
        return False
    name = path.name
    if not name.lower().endswith(".mkv"):
        return False
    return name.startswith(prefix)


def _replay_incoming_basename(source: Path, settings: Settings, prefix: str) -> str:
    """
    Ingest filename: drop the replay prefix from the OBS filename (case-insensitive), strip spaces.

    Falls back to :func:`ingest.incoming_clip_local_basename` if the remainder is empty or unsafe.
    """
    name = source.name
    pfx = prefix
    if pfx and name.lower().startswith(pfx.lower()):
        rest = name[len(pfx) :].strip()
    else:
        rest = ""
    if not rest or "/" in rest or "\\" in rest or rest in (".", ".."):
        return incoming_clip_local_basename(settings, ".mp4")
    stem = Path(rest).stem
    if not stem:
        return incoming_clip_local_basename(settings, ".mp4")
    return f"{stem}.mp4"


def _list_replay_candidates(folder: Path, settings: Settings, prefix: str) -> list[Path]:
    if not folder.is_dir():
        return []
    out: list[Path] = []
    for entry in folder.iterdir():
        if not _replay_matches(entry, prefix):
            continue
        if is_copying_temp_clip(entry, settings):
            continue
        if should_ignore_file(entry, settings):
            continue
        out.append(entry)
    return out


def _newest_acceptable(
    candidates: list[Path],
    *,
    trigger_ts: float,
    tolerance_seconds: float,
) -> Path | None:
    """Keep files whose mtime is not clearly older than ``trigger_ts`` (within tolerance)."""
    cutoff = trigger_ts - max(0.0, tolerance_seconds)
    scored: list[tuple[float, Path]] = []
    for p in candidates:
        try:
            m = p.stat().st_mtime
        except OSError as exc:
            logger.warning(
                "replay-buffer: skip candidate (stat failed)",
                extra={"structured": {"path": str(p), "error": str(exc)}},
            )
            continue
        if m < cutoff:
            logger.debug(
                "replay-buffer: skip candidate (mtime before trigger window)",
                extra={
                    "structured": {
                        "path": str(p),
                        "mtime": m,
                        "cutoff": cutoff,
                        "trigger_ts": trigger_ts,
                    }
                },
            )
            continue
        scored.append((m, p))
    if not scored:
        return None
    scored.sort(key=lambda t: t[0])
    return scored[-1][1]


def wait_stable_until_deadline(
    path: Path,
    settings: Settings,
    *,
    deadline_monotonic: float,
    stable_rounds_required: int = 2,
    log_context: str = "replay_buffer",
    check_delay: float | None = None,
    min_age_seconds: float | None = None,
    max_retries: int | None = None,
) -> tuple[bool, str | None]:
    """
    Same semantics as ``wait_until_stable_with_timing`` (size + mtime stable for
    ``stable_rounds_required`` checks), using ``settings.file_stable_check_seconds`` as
    the poll interval — not a fixed 500ms.

    Optional overrides (used by replay-buffer paths so replays do not inherit the global
    ``FILE_STABLE_*`` defaults, which are tuned for long recordings).

    Returns ``(True, None)`` on success, or ``(False, reason)`` on timeout or exhaustion.
    """
    delay = (
        settings.file_stable_check_seconds if check_delay is None else check_delay
    )
    min_age = (
        settings.file_stable_min_age_seconds if min_age_seconds is None else min_age_seconds
    )
    max_retries = (
        max(1, settings.file_stable_retries) if max_retries is None else max(1, max_retries)
    )

    previous_size: int | None = None
    previous_mtime: float | None = None
    stable_rounds = 0

    for round_idx in range(1, max_retries + 1):
        if time.monotonic() >= deadline_monotonic:
            logger.warning(
                "replay-buffer: stability wait aborted (deadline)",
                extra={
                    "structured": {
                        "path": str(path),
                        "round": round_idx,
                        "context": log_context,
                    }
                },
            )
            return False, "timeout_before_stable"

        if not path.exists() or path.is_dir():
            logger.error(
                "replay-buffer: candidate missing during stability wait",
                extra={"structured": {"path": str(path), "round": round_idx}},
            )
            return False, "file_disappeared_during_stability_wait"

        stat = path.stat()
        age_seconds = time.time() - stat.st_mtime

        if age_seconds < min_age:
            logger.info(
                "replay-buffer: file too new for stability check; waiting",
                extra={
                    "structured": {
                        "path": str(path),
                        "round": round_idx,
                        "age_seconds": round(age_seconds, 3),
                        "min_age_seconds": min_age,
                        "context": log_context,
                    }
                },
            )
            time.sleep(delay)
            continue

        if is_file_locked(path):
            logger.info(
                "replay-buffer: file locked; waiting",
                extra={
                    "structured": {
                        "path": str(path),
                        "round": round_idx,
                        "context": log_context,
                    }
                },
            )
            time.sleep(delay)
            continue

        if not path.exists() or path.is_dir():
            return False, "file_disappeared_during_stability_wait"

        stat = path.stat()
        current_size = stat.st_size
        current_mtime = stat.st_mtime

        if current_size == previous_size and current_mtime == previous_mtime:
            stable_rounds += 1
        else:
            stable_rounds = 0

        previous_size = current_size
        previous_mtime = current_mtime

        if stable_rounds >= stable_rounds_required:
            logger.info(
                "replay-buffer: file stabilized",
                extra={
                    "structured": {
                        "path": str(path),
                        "size_bytes": current_size,
                        "round": round_idx,
                        "stable_rounds": stable_rounds,
                        "context": log_context,
                    }
                },
            )
            return True, None

        logger.debug(
            "replay-buffer: file not yet stable",
            extra={
                "structured": {
                    "path": str(path),
                    "size_bytes": current_size,
                    "round": round_idx,
                    "stable_rounds": stable_rounds,
                    "context": log_context,
                }
            },
        )

        sleep_for = min(delay, max(0.0, deadline_monotonic - time.monotonic()))
        if sleep_for > 0:
            time.sleep(sleep_for)

    if path.exists() and path.is_file() and is_file_locked(path):
        logger.warning(
            "replay-buffer: file still locked after max retries",
            extra={"structured": {"path": str(path), "context": log_context}},
        )
        return False, "still_locked_after_max_retries"

    logger.warning(
        "replay-buffer: file did not stabilize within retry budget",
        extra={"structured": {"path": str(path), "context": log_context}},
    )
    return False, "unstable_after_max_retries"


def _replay_wait_stable(
    path: Path,
    settings: Settings,
    *,
    deadline_monotonic: float,
    log_context: str,
) -> tuple[bool, str | None]:
    """
    Stability wait using ``REPLAY_BUFFER_STABLE_*`` (not global ``FILE_STABLE_*``).

    Defaults target ~1–2s to a verified, atomically replaced scoreboard file while requiring
    consecutive matching size/mtime samples (no partial reads of a growing file).
    """
    return wait_stable_until_deadline(
        path,
        settings,
        deadline_monotonic=deadline_monotonic,
        stable_rounds_required=settings.replay_buffer_stable_rounds_required,
        log_context=log_context,
        check_delay=settings.replay_buffer_stable_check_seconds,
        min_age_seconds=settings.replay_buffer_stable_min_age_seconds,
        max_retries=settings.replay_buffer_stable_max_retries,
    )


def _scoreboard_replay_destination(settings: Settings) -> tuple[Path | None, str | None]:
    """
    Resolve the scoreboard file path (must resolve to basename ``INSTANTREPLAY.mkv``).
    """
    ir = settings.instant_replay_source
    if ir is None:
        logger.error(
            "replay-buffer: INSTANT_REPLAY_SOURCE / instant_replay_source is not configured",
        )
        return None, "instant_replay_source_not_configured"
    ir = ir.expanduser().resolve(strict=False)
    if ir.is_dir():
        dest = ir / SCOREBOARD_REPLAY_BASENAME
    else:
        dest = ir
    if dest.name.upper() != SCOREBOARD_REPLAY_BASENAME.upper():
        logger.error(
            "replay-buffer: scoreboard path must be named INSTANTREPLAY.mkv",
            extra={"structured": {"configured_path": str(dest)}},
        )
        return None, "scoreboard_path_must_be_INSTANTREPLAY.mkv"
    return dest, None


def _verify_nonempty_video(path: Path, *, role: str) -> tuple[bool, str | None, int]:
    """Confirm path exists, is a file, and has non-zero size."""
    try:
        if not path.is_file():
            logger.error(
                "replay-buffer: verification failed (not a file)",
                extra={"structured": {"role": role, "path": str(path)}},
            )
            return False, f"{role}_not_a_file", 0
        st = path.stat()
        if st.st_size <= 0:
            logger.error(
                "replay-buffer: verification failed (empty file)",
                extra={"structured": {"role": role, "path": str(path), "size": st.st_size}},
            )
            return False, f"{role}_empty_file", st.st_size
    except OSError as exc:
        logger.error(
            "replay-buffer: verification failed (stat error)",
            extra={"structured": {"role": role, "path": str(path), "error": str(exc)}},
        )
        return False, f"{role}_stat_failed", 0
    logger.info(
        "replay-buffer: verified destination",
        extra={"structured": {"role": role, "path": str(path), "size_bytes": st.st_size}},
    )
    return True, None, st.st_size


def _scoreboard_write_temp_path(final_path: Path) -> Path:
    """Unique temp path alongside ``final_path`` (same directory) for Windows-safe atomic replace."""
    parent = final_path.parent
    stem = final_path.stem
    return parent / f"{stem}.{secrets.token_hex(8)}.tmp"


def _unlink_ignore_missing(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _incoming_remux_temp_path(final_mp4: Path) -> Path:
    """Temp path skipped by the watcher (:func:`processor.is_copying_temp_clip`)."""
    return final_mp4.parent / f"{final_mp4.stem}.{secrets.token_hex(6)}.copying.mp4"


def _replay_remux_failed_move_source(
    settings: Settings,
    src_resolved: Path,
    detected_at: str,
    remux_err: str | None,
) -> ProcessLatestReplayResult:
    try:
        failed_dest = unique_destination(settings.failed_folder, src_resolved.name)
        move_with_retries(
            src_resolved,
            failed_dest,
            retries=settings.move_retries,
            delay_seconds=settings.move_retry_delay_seconds,
        )
        logger.warning(
            "replay-buffer: moved long_clips replay to failed after remux exhaustion",
            extra={
                "structured": {
                    "from": str(src_resolved),
                    "to": str(failed_dest),
                }
            },
        )
    except Exception as exc:
        logger.exception(
            "replay-buffer: remux failed and could not move source to failed folder",
            extra={"structured": {"source": str(src_resolved)}},
        )
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason="replay_buffer_remux_failed_move_to_failed_failed",
            scoreboard_replay_path=None,
            processing_error=str(exc)[:500],
            source_deleted=False,
        )
    return ProcessLatestReplayResult(
        success=False,
        selected_source_path=str(src_resolved),
        incoming_path=None,
        detected_at=detected_at,
        stability_confirmed=True,
        failure_reason="replay_buffer_remux_failed",
        scoreboard_replay_path=None,
        processing_error=remux_err,
        source_deleted=False,
    )


def _atomic_replace_scoreboard_temp(
    temp_path: Path,
    final_path: Path,
) -> tuple[bool, str | None]:
    """
    Replace ``final_path`` with verified content at ``temp_path`` using ``os.replace``
    (atomic on same volume for Windows and POSIX readers of ``final_path``).
    On failure, ``final_path`` is left unchanged when the OS provides that guarantee.
    """
    logger.info(
        "replay-buffer: scoreboard atomic replace starting",
        extra={
            "structured": {
                "temp_path": str(temp_path),
                "final_path": str(final_path),
            }
        },
    )
    try:
        os.replace(temp_path, final_path)
    except OSError as exc:
        logger.exception(
            "replay-buffer: scoreboard atomic replace failed; "
            "prior INSTANTREPLAY.mkv left unchanged when possible",
            extra={
                "structured": {
                    "temp_path": str(temp_path),
                    "final_path": str(final_path),
                }
            },
        )
        return False, str(exc)[:500]
    logger.info(
        "replay-buffer: scoreboard atomic replace completed",
        extra={"structured": {"final_path": str(final_path)}},
    )
    return True, None


def _replay_buffer_dual_copy_and_delete_source(
    source: Path,
    settings: Settings,
    *,
    detected_at: str,
    filename_prefix: str,
) -> ProcessLatestReplayResult:
    """
    Replay-buffer only: remux stable ``source`` (Matroska) to an incoming ``*.copying.mp4`` temp,
    copy ``source`` to a scoreboard temp and atomically replace ``INSTANTREPLAY.mkv``, atomically
    place the remuxed MP4 in ``clips_incoming``, verify, then delete ``source``.

    If remux never produces a valid MP4 after configured retries, the long_clips ``source`` is moved
    to ``FAILED_FOLDER`` (scoreboard is not updated). If the scoreboard update fails after a good
    remux temp exists, ``source`` is left in long_clips and the temp remux file is removed.

    Incoming basename = ``source`` name with ``filename_prefix`` removed (see :func:`_replay_incoming_basename`).
    """
    src_resolved = source.resolve(strict=False)
    scoreboard_dest, cfg_err = _scoreboard_replay_destination(settings)
    if cfg_err is not None or scoreboard_dest is None:
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason=cfg_err,
            scoreboard_replay_path=None,
            source_deleted=False,
        )

    incoming_dir = settings.clips_incoming_folder.resolve(strict=False)
    incoming_dir.mkdir(parents=True, exist_ok=True)
    incoming_name = _replay_incoming_basename(source, settings, filename_prefix)
    incoming_dest = incoming_dir / incoming_name
    if incoming_dest.exists():
        incoming_dest = unique_destination(incoming_dir, incoming_name)

    incoming_remux_temp = _incoming_remux_temp_path(incoming_dest)
    _unlink_ignore_missing(incoming_remux_temp)

    logger.info(
        "replay-buffer: remux to incoming temp (stream copy to mp4)",
        extra={
            "structured": {
                "source": str(src_resolved),
                "temp": str(incoming_remux_temp),
                "final_incoming": str(incoming_dest),
            }
        },
    )
    ok_remux, remux_err = remux_to_mp4_with_retries(
        settings,
        src_resolved,
        incoming_remux_temp,
        log_context="replay_buffer_incoming",
    )
    if not ok_remux:
        logger.error(
            "replay-buffer: remux exhausted; moving long_clips source to failed when possible",
            extra={"structured": {"source": str(src_resolved)}},
        )
        return _replay_remux_failed_move_source(settings, src_resolved, detected_at, remux_err)

    ok_rmx, reason_rmx, _ = _verify_nonempty_video(incoming_remux_temp, role="incoming_remux_temp")
    if not ok_rmx:
        logger.error(
            "replay-buffer: remux temp verification failed; moving source to failed",
            extra={"structured": {"reason": reason_rmx, "path": str(incoming_remux_temp)}},
        )
        _unlink_ignore_missing(incoming_remux_temp)
        return _replay_remux_failed_move_source(settings, src_resolved, detected_at, reason_rmx)

    scoreboard_dest.parent.mkdir(parents=True, exist_ok=True)
    scoreboard_temp = _scoreboard_write_temp_path(scoreboard_dest)

    logger.info(
        "replay-buffer: copying to scoreboard temp (no direct write to INSTANTREPLAY.mkv)",
        extra={
            "structured": {
                "source": str(src_resolved),
                "scoreboard_temp": str(scoreboard_temp),
                "scoreboard_final": str(scoreboard_dest),
            }
        },
    )
    try:
        shutil.copy2(src_resolved, scoreboard_temp)
    except OSError as exc:
        logger.exception(
            "replay-buffer: copy to scoreboard temp failed; source file not deleted",
            extra={"structured": {"source": str(src_resolved)}},
        )
        _unlink_ignore_missing(scoreboard_temp)
        _unlink_ignore_missing(incoming_remux_temp)
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason="copy_to_scoreboard_temp_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            processing_error=str(exc)[:500],
            source_deleted=False,
        )

    ok_sb, reason_sb, _ = _verify_nonempty_video(scoreboard_temp, role="scoreboard_temp")
    if not ok_sb:
        logger.error(
            "replay-buffer: scoreboard temp verification failed; source file not deleted",
            extra={"structured": {"reason": reason_sb, "path": str(scoreboard_temp)}},
        )
        _unlink_ignore_missing(scoreboard_temp)
        _unlink_ignore_missing(incoming_remux_temp)
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason=reason_sb or "scoreboard_temp_verify_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            source_deleted=False,
        )

    ok_ar, ar_err = _atomic_replace_scoreboard_temp(scoreboard_temp, scoreboard_dest)
    if not ok_ar:
        _unlink_ignore_missing(incoming_remux_temp)
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason="scoreboard_atomic_replace_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            processing_error=ar_err,
            source_deleted=False,
        )

    logger.info(
        "replay-buffer: placing remuxed mp4 in incoming",
        extra={
            "structured": {
                "incoming_temp": str(incoming_remux_temp),
                "incoming_dest": str(incoming_dest),
            }
        },
    )
    try:
        os.replace(incoming_remux_temp, incoming_dest)
    except OSError as exc:
        logger.exception(
            "replay-buffer: could not place remuxed file in incoming "
            "(scoreboard INSTANTREPLAY.mkv already replaced)",
            extra={
                "structured": {
                    "incoming_temp": str(incoming_remux_temp),
                    "incoming_dest": str(incoming_dest),
                }
            },
        )
        _unlink_ignore_missing(incoming_remux_temp)
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason="incoming_remux_final_replace_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            processing_error=str(exc)[:500],
            source_deleted=False,
        )

    ok_in, reason_in, _ = _verify_nonempty_video(incoming_dest, role="incoming")
    if not ok_in:
        logger.error(
            "replay-buffer: incoming verification failed; source file not deleted",
            extra={"structured": {"reason": reason_in, "path": str(incoming_dest)}},
        )
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=str(incoming_dest.resolve(strict=False)),
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason=reason_in or "incoming_verify_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            source_deleted=False,
        )

    ok_sb2, _, _ = _verify_nonempty_video(scoreboard_dest, role="scoreboard_final")
    ok_in2, _, _ = _verify_nonempty_video(incoming_dest, role="incoming_final")
    if not (ok_sb2 and ok_in2):
        logger.error(
            "replay-buffer: post-verify check failed; source file not deleted",
            extra={
                "structured": {
                    "scoreboard_ok": ok_sb2,
                    "incoming_ok": ok_in2,
                }
            },
        )
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(src_resolved),
            incoming_path=str(incoming_dest.resolve(strict=False)),
            detected_at=detected_at,
            stability_confirmed=True,
            failure_reason="destination_post_verify_failed",
            scoreboard_replay_path=str(scoreboard_dest),
            source_deleted=False,
        )

    deleted = False
    try:
        src_resolved.unlink()
        deleted = True
        logger.info(
            "replay-buffer: removed source replay from long_clips after successful remux/promotion",
            extra={"structured": {"source": str(src_resolved)}},
        )
    except OSError as exc:
        logger.warning(
            "replay-buffer: remux/promotion succeeded but could not delete long_clips source",
            exc_info=True,
            extra={
                "structured": {
                    "source": str(src_resolved),
                    "error": str(exc)[:500],
                }
            },
        )

    incoming_final = incoming_dest.resolve(strict=False)
    return ProcessLatestReplayResult(
        success=True,
        selected_source_path=str(src_resolved),
        incoming_path=str(incoming_final),
        detected_at=detected_at,
        stability_confirmed=True,
        failure_reason=None,
        scoreboard_replay_path=str(scoreboard_dest.resolve(strict=False)),
        source_deleted=deleted,
    )


def run_process_latest_replay(
    settings: Settings,
    *,
    trigger_ts: float,
    timeout_seconds: float,
    filename_prefix: str,
    tolerance_seconds: float,
    request_id: str | None = None,
) -> ProcessLatestReplayResult:
    """
    Replay-buffer path only: poll ``settings.long_clips_folder`` for ``{prefix}*.mkv``,
    select the newest acceptable file, stabilize, then remux + scoreboard copy + verified delete via
    :func:`_replay_buffer_dual_copy_and_delete_source`.

    Does not call ``process_clip``; the caller runs the pipeline on ``incoming_path``.
    """
    folder = settings.long_clips_folder
    if folder is None:
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=None,
            incoming_path=None,
            detected_at=None,
            stability_confirmed=False,
            failure_reason="long_clips_folder_not_configured",
            source_deleted=False,
        )

    folder = folder.resolve(strict=False)
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    prefix = filename_prefix

    logger.info(
        "replay-buffer: starting scan",
        extra={
            "structured": _replay_corr_struct(
                request_id,
                {
                    "long_clips_folder": str(folder),
                    "trigger_ts": trigger_ts,
                    "trigger_iso": datetime.fromtimestamp(trigger_ts, tz=timezone.utc).isoformat(),
                    "timeout_seconds": timeout_seconds,
                    "filename_prefix": prefix,
                    "tolerance_seconds": tolerance_seconds,
                    "replay_stable_check_seconds": settings.replay_buffer_stable_check_seconds,
                    "replay_stable_min_age_seconds": settings.replay_buffer_stable_min_age_seconds,
                    "replay_stable_rounds": settings.replay_buffer_stable_rounds_required,
                },
            )
        },
    )

    selected: Path | None = None
    detected_at: str | None = None

    poll_round = 0
    while time.monotonic() < deadline:
        poll_round += 1
        candidates = _list_replay_candidates(folder, settings, prefix)
        pick = _newest_acceptable(
            candidates,
            trigger_ts=trigger_ts,
            tolerance_seconds=tolerance_seconds,
        )
        if pick is not None:
            selected = pick
            detected_at = datetime.now(timezone.utc).isoformat()
            logger.info(
                "replay-buffer: selected newest acceptable candidate",
                extra={
                    "structured": _replay_corr_struct(
                        request_id,
                        {
                            "path": str(selected),
                            "poll_round": poll_round,
                            "detected_at": detected_at,
                            "mtime": selected.stat().st_mtime,
                        },
                    )
                },
            )
            break
        logger.debug(
            "replay-buffer: no acceptable candidate yet",
            extra={
                "structured": {
                    "poll_round": poll_round,
                    "candidate_count": len(candidates),
                    "folder": str(folder),
                }
            },
        )
        sleep_for = min(
            settings.replay_buffer_stable_check_seconds,
            max(0.0, deadline - time.monotonic()),
        )
        if sleep_for <= 0:
            break
        time.sleep(sleep_for)

    if selected is None:
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=None,
            incoming_path=None,
            detected_at=None,
            stability_confirmed=False,
            failure_reason="no_matching_file_within_timeout",
            source_deleted=False,
        )

    ok, stab_reason = _replay_wait_stable(
        selected,
        settings,
        deadline_monotonic=deadline,
        log_context="replay_buffer",
    )
    if not ok:
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=str(selected.resolve(strict=False)),
            incoming_path=None,
            detected_at=detected_at,
            stability_confirmed=False,
            failure_reason=stab_reason,
            source_deleted=False,
        )

    assert detected_at is not None
    with replay_promotion_lock:
        return _replay_buffer_dual_copy_and_delete_source(
            selected,
            settings,
            detected_at=detected_at,
            filename_prefix=prefix,
        )


def tick_replay_scoreboard_auto_sync(
    settings: Settings,
    submit: Callable[[Path], None],
    *,
    filename_prefix: str,
) -> None:
    """
    One pass: newest stable ``{prefix}*.mkv`` in long_clips → scoreboard + remuxed incoming mp4, then ``submit``.
    Used by the worker when ``REPLAY_SCOREBOARD_AUTO_SYNC_INTERVAL_SECONDS`` > 0.
    """
    if settings.long_clips_folder is None or settings.instant_replay_source is None:
        return
    folder = settings.long_clips_folder.resolve(strict=False)
    if not folder.is_dir():
        return

    candidates = _list_replay_candidates(folder, settings, filename_prefix)
    if not candidates:
        return

    scored: list[tuple[float, Path]] = []
    for p in candidates:
        try:
            scored.append((p.stat().st_mtime, p))
        except OSError:
            continue
    if not scored:
        return
    scored.sort(key=lambda t: t[0])
    newest = scored[-1][1]

    budget = min(
        45.0,
        max(
            8.0,
            settings.replay_buffer_stable_check_seconds
            * max(8, settings.replay_buffer_stable_max_retries // 2),
        ),
    )
    deadline = time.monotonic() + budget
    ok, stab_reason = _replay_wait_stable(
        newest,
        settings,
        deadline_monotonic=deadline,
        log_context="replay_buffer_auto",
    )
    if not ok:
        logger.debug(
            "replay-buffer: auto-sync skipped (not stable yet)",
            extra={"structured": {"path": str(newest), "reason": stab_reason}},
        )
        return

    detected_at = datetime.now(timezone.utc).isoformat()
    with replay_promotion_lock:
        result = _replay_buffer_dual_copy_and_delete_source(
            newest,
            settings,
            detected_at=detected_at,
            filename_prefix=filename_prefix,
        )

    if result.success and result.incoming_path:
        logger.info(
            "replay-buffer: auto-sync promoted replay; submitting incoming clip",
            extra={
                "structured": {
                    "incoming_path": result.incoming_path,
                    "scoreboard_replay_path": result.scoreboard_replay_path,
                }
            },
        )
        submit(Path(result.incoming_path))
    elif not result.success:
        logger.warning(
            "replay-buffer: auto-sync promote failed",
            extra={
                "structured": {
                    "failure_reason": result.failure_reason,
                    "path": str(newest),
                }
            },
        )


def replay_scoreboard_auto_sync_loop(
    settings: Settings,
    submit: Callable[[Path], None],
    stop: threading.Event,
) -> None:
    """Poll ``tick_replay_scoreboard_auto_sync`` until ``stop`` is set."""
    interval = max(0.05, float(settings.replay_scoreboard_auto_sync_interval_seconds))
    prefix = settings.replay_buffer_filename_prefix
    logger.info(
        "replay-buffer: auto scoreboard sync loop started",
        extra={"structured": {"interval_seconds": interval, "filename_prefix": prefix}},
    )
    while not stop.is_set():
        if stop.wait(timeout=interval):
            break
        try:
            tick_replay_scoreboard_auto_sync(settings, submit, filename_prefix=prefix)
        except Exception:
            logger.exception("replay-buffer: auto-sync tick failed")


def run_process_latest_replay_cli(
    settings: Settings,
    *,
    trigger_raw: str,
    timeout_seconds: float,
    filename_prefix: str,
    tolerance_seconds: float,
    request_id: str | None = None,
) -> ProcessLatestReplayResult:
    """Parse trigger and run :func:`run_process_latest_replay`."""
    try:
        trigger_ts = parse_trigger_timestamp(trigger_raw)
    except ValueError as exc:
        logger.warning(
            "replay-buffer: invalid trigger timestamp",
            extra={
                "structured": _replay_corr_struct(
                    request_id,
                    {"trigger_raw": trigger_raw[:200], "error": str(exc)[:500]},
                )
            },
        )
        return ProcessLatestReplayResult(
            success=False,
            selected_source_path=None,
            incoming_path=None,
            detected_at=None,
            stability_confirmed=False,
            failure_reason="invalid_trigger_timestamp",
            processing_error=str(exc)[:500],
            source_deleted=False,
        )
    return run_process_latest_replay(
        settings,
        trigger_ts=trigger_ts,
        timeout_seconds=timeout_seconds,
        filename_prefix=filename_prefix,
        tolerance_seconds=tolerance_seconds,
        request_id=request_id,
    )
