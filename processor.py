"""
End-to-end processing for a single clip: stabilize, rename to UTC, preview, upload, DB, move originals.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from supabase import Client

from config import Settings, slug_from_stem
from database import update_clip_booking_id, upsert_clip_record
from job_store import (
    JobIdempotencyCollisionError,
    JobStore,
    STEP_BOOKING,
    STEP_DB_UPSERT,
    STEP_FINALIZED,
    STEP_PREVIEW,
    STEP_RENAMED_UTC,
    STEP_UPLOAD_ORIGINAL,
    STEP_UPLOAD_PREVIEW,
    make_idempotency_key,
)
from uploader import S3Uploader

logger = logging.getLogger(__name__)

_ACTIVE_PATHS_LOCK = threading.Lock()
_ACTIVE_PATHS: set[str] = set()

_RECENT_FAILURES_LOCK = threading.Lock()
_RECENT_FAILURES: dict[str, float] = {}

_FFMPEG_SOFT_FAILS_LOCK = threading.Lock()
_FFMPEG_SOFT_FAILS: dict[str, int] = {}

_COMPLETED_CLIPS_LOCK = threading.Lock()
_COMPLETED_CLIP_UNTIL: dict[str, float] = {}

# Full filename: YYYY-MM-DDTHH-MM-SSZ.mp4 (Z may be z; extension must be a configured video ext).
UTC_CLIP_FILENAME_RE = re.compile(
    r"^(?P<dt>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})(?P<z>[zZ])(?P<ext>\.[^.]+)$",
    re.IGNORECASE,
)


class FileLockedError(RuntimeError):
    """Raised when a file remains locked by another process."""


class FileStillChangingError(RuntimeError):
    """Raised when a file keeps changing and never stabilizes."""


class FfmpegDecodeError(RuntimeError):
    """ffmpeg failed with output that looks like corrupt/incomplete media."""


def _path_key(path: Path) -> str:
    return str(path.resolve(strict=False)).lower()


def clip_path_inflight(path: Path) -> bool:
    """True if this normalized path is already claimed for active ``process_clip`` (or rename alias)."""
    key = _path_key(path)
    with _ACTIVE_PATHS_LOCK:
        return key in _ACTIVE_PATHS


def _owns_active_processing_claim(original_input_path: Path) -> bool:
    """True if this invocation still holds the original claim (not released in ``finally`` yet)."""
    return clip_path_inflight(original_input_path)


def _claim_path(path: Path) -> bool:
    key = _path_key(path)
    with _ACTIVE_PATHS_LOCK:
        if key in _ACTIVE_PATHS:
            return False
        _ACTIVE_PATHS.add(key)
        return True


def _release_path(path: Path) -> None:
    key = _path_key(path)
    with _ACTIVE_PATHS_LOCK:
        _ACTIVE_PATHS.discard(key)


def _register_inflight_alias(path: Path) -> None:
    """Reserve a destination path key before rename/move so duplicates cannot claim it."""
    key = _path_key(path)
    with _ACTIVE_PATHS_LOCK:
        _ACTIVE_PATHS.add(key)


def _prune_completed_clip_cache(now: float) -> None:
    stale = [k for k, until in _COMPLETED_CLIP_UNTIL.items() if until <= now]
    for k in stale:
        _COMPLETED_CLIP_UNTIL.pop(k, None)


def mark_clip_recently_completed(path: Path, ttl_seconds: float) -> None:
    """Suppress duplicate watcher/submit noise for a clip that finished successfully."""
    if ttl_seconds <= 0:
        return
    key = _path_key(path)
    with _COMPLETED_CLIPS_LOCK:
        _prune_completed_clip_cache(time.monotonic())
        _COMPLETED_CLIP_UNTIL[key] = time.monotonic() + ttl_seconds


def is_recently_completed_clip(path: Path) -> bool:
    key = _path_key(path)
    now = time.monotonic()
    with _COMPLETED_CLIPS_LOCK:
        _prune_completed_clip_cache(now)
        until = _COMPLETED_CLIP_UNTIL.get(key)
        if until is None:
            return False
        if until <= now:
            _COMPLETED_CLIP_UNTIL.pop(key, None)
            return False
        return True


def clip_filename_matches_utc_layout(path: Path, settings: Settings | None = None) -> bool:
    """
    True if the basename matches ``YYYY-MM-DDTHH-MM-SSZ`` + video extension (regex + ext only).

    Used for idempotent UTC rename: must not call ``strptime`` with the local OBS format
    (``%Y-%m-%dT%H-%M-%S``), which leaves a trailing ``Z`` unconverted.
    """
    m = UTC_CLIP_FILENAME_RE.match(path.name)
    if not m:
        return False
    ext = m.group("ext").lower()
    if settings is not None and ext not in settings.video_extensions:
        return False
    if settings is None and ext not in (".mp4", ".mov"):
        return False
    return True


def is_utc_obs_clip_filename(path: Path, settings: Settings | None = None) -> bool:
    """
    True if the name is already our UTC clip pattern ``YYYY-MM-DDTHH-MM-SSZ`` + video extension.
    Layout-only (regex + allowed extension); parsing for metadata uses ``parse_captured_at_utc``.
    """
    return clip_filename_matches_utc_layout(path, settings)


def _mark_recent_failure(path: Path) -> None:
    with _RECENT_FAILURES_LOCK:
        _RECENT_FAILURES[_path_key(path)] = time.time()


def _clear_recent_failure(path: Path) -> None:
    with _RECENT_FAILURES_LOCK:
        _RECENT_FAILURES.pop(_path_key(path), None)


def _clear_ffmpeg_soft_fails(path: Path) -> None:
    with _FFMPEG_SOFT_FAILS_LOCK:
        _FFMPEG_SOFT_FAILS.pop(_path_key(path), None)


def _is_in_recent_failure_cooldown(path: Path, settings: Settings) -> bool:
    cooldown = settings.recent_failure_cooldown_seconds
    if cooldown <= 0:
        return False

    key = _path_key(path)
    now = time.time()

    with _RECENT_FAILURES_LOCK:
        last_failure = _RECENT_FAILURES.get(key)
        if last_failure is None:
            return False

        if (now - last_failure) < cooldown:
            return True

        del _RECENT_FAILURES[key]
        return False


def is_video_file(path: Path, settings: Settings) -> bool:
    return path.suffix.lower() in settings.video_extensions


def is_copying_temp_clip(path: Path, settings: Settings) -> bool:
    """
    True for ingest temp files ``*.copying.mp4`` / ``*.copying.mov`` / ``*.<stem>.copying.<ext>``
    for any configured video extension. Never enqueue or process these.
    """
    n = path.name.lower()
    for ext in settings.video_extensions:
        e = ext.lower()
        if not e.startswith("."):
            e = "." + e
        if n.endswith(f".copying{e}"):
            return True
    return False


def should_ignore_file(path: Path, settings: Settings) -> bool:
    if is_copying_temp_clip(path, settings):
        return True

    name = path.name

    if name in settings.ignore_filenames:
        return True

    for prefix in settings.ignore_prefixes:
        if name.startswith(prefix):
            return True

    suffix_lower = name.lower()
    for suffix in settings.ignore_suffixes:
        if suffix_lower.endswith(suffix.lower()):
            return True

    return False


def clip_readiness_gate(path: Path, settings: Settings) -> bool:
    """
    Before full stabilization, require min age, unlocked, and unchanged size/mtime across
    several consecutive checks. Returns False if the clip should be left for a later retry.
    """
    delay = settings.file_stable_check_seconds
    min_age = settings.file_stable_min_age_seconds
    rounds_needed = settings.clip_readiness_stable_rounds
    max_cycles = settings.clip_readiness_max_cycles

    prev_size: int | None = None
    prev_mtime: float | None = None
    stable_matches = 0

    for cycle in range(1, max_cycles + 1):
        if not path.exists() or path.is_dir():
            logger.info(
                "Readiness gate: clip missing",
                extra={"structured": {"path": str(path), "cycle": cycle}},
            )
            return False

        if is_file_locked(path):
            logger.info(
                "Deferred clip: file locked during readiness gate",
                extra={"structured": {"path": str(path), "cycle": cycle}},
            )
            return False

        stat = path.stat()
        age_seconds = time.time() - stat.st_mtime
        if age_seconds < min_age:
            logger.info(
                "Deferred clip: file too new for readiness gate",
                extra={
                    "structured": {
                        "path": str(path),
                        "cycle": cycle,
                        "age_seconds": round(age_seconds, 3),
                        "min_age_seconds": min_age,
                    }
                },
            )
            time.sleep(delay)
            prev_size, prev_mtime = None, None
            stable_matches = 0
            continue

        current_size = stat.st_size
        current_mtime = stat.st_mtime

        if (
            prev_size is not None
            and prev_mtime is not None
            and current_size == prev_size
            and current_mtime == prev_mtime
        ):
            stable_matches += 1
        else:
            stable_matches = 0

        prev_size = current_size
        prev_mtime = current_mtime

        if stable_matches >= rounds_needed:
            logger.info(
                "Readiness gate passed",
                extra={
                    "structured": {
                        "path": str(path),
                        "cycle": cycle,
                        "stable_matches": stable_matches,
                        "size_bytes": current_size,
                    }
                },
            )
            return True

        logger.debug(
            "Readiness gate: file not yet steady",
            extra={
                "structured": {
                    "path": str(path),
                    "cycle": cycle,
                    "stable_matches": stable_matches,
                    "size_bytes": current_size,
                }
            },
        )
        time.sleep(delay)

    logger.info(
        "Deferred clip: readiness gate timed out without stable rounds",
        extra={"structured": {"path": str(path), "max_cycles": max_cycles}},
    )
    return False


def is_file_locked(path: Path) -> bool:
    """
    Best-effort Windows-friendly lock test.
    Attempts to open the file for append. If another process still has an exclusive
    lock on it, this will usually fail on Windows.
    """
    if not path.exists() or path.is_dir():
        return False

    try:
        with open(path, "ab"):
            return False
    except OSError:
        return True


def wait_until_stable_with_timing(
    path: Path,
    *,
    delay: float,
    retries: int,
    min_age: float,
    stable_rounds_required: int = 2,
    log_context: str = "clip",
) -> None:
    """
    Wait until the file exists, is at least ``min_age`` old, is not locked, and has
    unchanged size/mtime across ``stable_rounds_required`` consecutive checks.
    """
    previous_size: int | None = None
    previous_mtime: float | None = None
    stable_rounds = 0

    for round_idx in range(1, retries + 1):
        if not path.exists() or path.is_dir():
            raise FileNotFoundError(f"Clip not found (yet): {path}")

        stat = path.stat()
        age_seconds = time.time() - stat.st_mtime

        if age_seconds < min_age:
            logger.info(
                "File too new for stabilization check; waiting",
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
                "File still locked; waiting",
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
            raise FileNotFoundError(f"Clip disappeared during stabilization: {path}")

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
                "File stabilized",
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
            return

        logger.debug(
            "File not yet stable",
            extra={
                "structured": {
                    "path": str(path),
                    "size_bytes": current_size,
                    "mtime": current_mtime,
                    "round": round_idx,
                    "stable_rounds": stable_rounds,
                    "context": log_context,
                }
            },
        )

        time.sleep(delay)

    if is_file_locked(path):
        raise FileLockedError(f"File remained locked after {retries} checks: {path}")

    raise FileStillChangingError(
        f"File did not stabilize within {retries} checks: {path}"
    )


def wait_until_stable(path: Path, settings: Settings) -> None:
    """
    Wait until the file:
    - exists
    - is old enough
    - is not locked
    - has unchanged size and mtime across two consecutive checks
    """
    wait_until_stable_with_timing(
        path,
        delay=settings.file_stable_check_seconds,
        retries=settings.file_stable_retries,
        min_age=settings.file_stable_min_age_seconds,
        stable_rounds_required=2,
        log_context="clip",
    )


def unique_destination(dest_dir: Path, filename: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    candidate = dest_dir / filename
    if not candidate.exists():
        return candidate

    stem = Path(filename).stem
    suffix = Path(filename).suffix
    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    unique = dest_dir / f"{stem}_{ts}_{uuid.uuid4().hex[:8]}{suffix}"
    while unique.exists():
        unique = dest_dir / f"{stem}_{ts}_{uuid.uuid4().hex[:12]}{suffix}"
    return unique


def move_if_exists(
    src: Path,
    dest: Path,
    *,
    retries: int,
    delay_seconds: float,
    context: str,
) -> Path | None:
    """
    Like move_with_retries, but if ``src`` is already gone (e.g. another worker finished
    the lifecycle), log and return None instead of retrying.
    """
    if not src.exists() or not src.is_file():
        logger.warning(
            "Move skipped: source missing (likely already moved)",
            extra={
                "structured": {
                    "context": context,
                    "src": str(src),
                    "dest": str(dest),
                }
            },
        )
        return None
    return move_with_retries(
        src,
        dest,
        retries=retries,
        delay_seconds=delay_seconds,
    )


def move_with_retries(
    src: Path,
    dest: Path,
    *,
    retries: int,
    delay_seconds: float,
) -> Path:
    last: Exception | None = None
    attempts = max(1, retries)

    for attempt in range(1, attempts + 1):
        if not src.exists() or not src.is_file():
            logger.warning(
                "Move skipped: source missing (not retrying)",
                extra={
                    "structured": {
                        "from": str(src),
                        "to": str(dest),
                        "attempt": attempt,
                    }
                },
            )
            raise FileNotFoundError(f"Source missing for move: {src}")
        try:
            shutil.move(str(src), str(dest))
            logger.info(
                "Moved file",
                extra={
                    "structured": {
                        "from": str(src),
                        "to": str(dest),
                        "attempt": attempt,
                    }
                },
            )
            return dest
        except (PermissionError, OSError) as exc:
            last = exc
            logger.warning(
                "Move failed; retrying",
                extra={
                    "structured": {
                        "from": str(src),
                        "to": str(dest),
                        "attempt": attempt,
                        "error": str(exc),
                    }
                },
                exc_info=attempt == attempts,
            )
            if attempt < attempts and delay_seconds > 0:
                time.sleep(delay_seconds)

    assert last is not None
    raise RuntimeError(f"Could not move file after {attempts} attempts: {src} -> {dest}") from last


def convert_local_filename_to_utc_name(path: Path, local_tz_name: str, settings: Settings) -> str:
    """
    Convert a local-time OBS filename like:
        2026-04-04T14-12-13.mp4
    into a UTC filename like:
        2026-04-04T19-12-13Z.mp4

    Preserves the original file extension.
    Does not parse stems that already end in Z (UTC); returns ``path.name`` unchanged.
    """
    if clip_filename_matches_utc_layout(path, settings):
        return path.name

    stem = path.stem
    suffix = path.suffix.lower()

    if stem.endswith("Z") or stem.endswith("z"):
        raise ValueError(
            f"Filename looks like UTC but failed validation: {path.name}"
        )

    try:
        local_dt = datetime.strptime(stem, "%Y-%m-%dT%H-%M-%S")
    except ValueError as exc:
        raise ValueError(f"Filename is not in expected local OBS format: {path.name}") from exc

    local_zone = ZoneInfo(local_tz_name)
    local_dt = local_dt.replace(tzinfo=local_zone)
    utc_dt = local_dt.astimezone(timezone.utc)

    return utc_dt.strftime("%Y-%m-%dT%H-%M-%SZ") + suffix


def rename_clip_to_utc_filename(
    clip_path: Path,
    *,
    settings: Settings,
    local_tz_name: str,
    retries: int,
    delay_seconds: float,
) -> Path:
    """
    Rename a fully written local clip file from local-time OBS naming to UTC naming.
    Idempotent: if the name already matches ``YYYY-MM-DDTHH-MM-SSZ`` + video ext, returns unchanged.
    Registers the destination path in the in-flight set before moving to close rename races.
    """
    if clip_filename_matches_utc_layout(clip_path, settings):
        logger.info(
            "Clip already has UTC filename; skipping rename",
            extra={"structured": {"path": str(clip_path)}},
        )
        return clip_path

    new_name = convert_local_filename_to_utc_name(
        clip_path, local_tz_name=local_tz_name, settings=settings
    )

    if clip_path.name == new_name:
        return clip_path

    dest = unique_destination(clip_path.parent, new_name)
    _register_inflight_alias(dest)
    try:
        renamed = move_with_retries(
            clip_path,
            dest,
            retries=retries,
            delay_seconds=delay_seconds,
        )
    except Exception:
        _release_path(dest)
        raise

    logger.info(
        "Renamed clip to UTC filename",
        extra={
            "structured": {
                "from": str(clip_path),
                "to": str(renamed),
                "local_timezone": local_tz_name,
            }
        },
    )
    return renamed


def build_s3_keys(settings: Settings, original_name: str, preview_name: str) -> tuple[str, str]:
    orig_prefix = settings.s3_original_prefix.strip("/")
    prev_prefix = settings.s3_preview_prefix.strip("/")

    orig_key = f"{orig_prefix}/{original_name}" if orig_prefix else original_name
    prev_key = f"{prev_prefix}/{preview_name}" if prev_prefix else preview_name

    return orig_key, prev_key


def resolve_ffmpeg_path(settings: Settings) -> str:
    ffmpeg_path = Path(str(settings.ffmpeg_path))

    if ffmpeg_path.exists() and ffmpeg_path.is_file():
        return str(ffmpeg_path)

    discovered = shutil.which(str(settings.ffmpeg_path))
    if discovered:
        return discovered

    discovered_plain = shutil.which("ffmpeg")
    if discovered_plain:
        return discovered_plain

    raise FileNotFoundError(
        f"ffmpeg executable not found. Checked settings.ffmpeg_path={settings.ffmpeg_path!r} and PATH."
    )


def _stderr_suggests_decode_corruption(stderr: str) -> bool:
    if not stderr:
        return False
    low = stderr.lower()
    markers = (
        "invalid nal unit",
        "nal unit size",
        "error splitting the input into nal units",
        "invalid data found when processing input",
        "nothing was written into output file",
    )
    return any(m in low for m in markers)


def run_ffmpeg_preview(
    settings: Settings,
    input_path: Path,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    vf = f"scale={settings.preview_width}:-2"
    ffmpeg_exe = resolve_ffmpeg_path(settings)

    cmd = [
        ffmpeg_exe,
        "-y",
        "-i",
        str(input_path),
        "-vf",
        vf,
        "-preset",
        settings.preview_preset,
        "-crf",
        str(settings.preview_crf),
        "-an",
        str(output_path),
    ]
    logger.info(
        "Running ffmpeg",
        extra={
            "structured": {
                "cmd": " ".join(cmd),
                "input": str(input_path),
                "output": str(output_path),
            }
        },
    )
    run_kw: dict[str, object] = {
        "capture_output": True,
        "text": True,
        "check": False,
        "stdin": subprocess.DEVNULL,
    }
    if sys.platform == "win32":
        # Avoid a visible console window that can linger after ffmpeg exits.
        run_kw["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    proc = subprocess.run(cmd, **run_kw)
    if proc.returncode != 0:
        stderr = (proc.stderr or "")[-4000:]
        stdout = (proc.stdout or "")[-2000:]
        if _stderr_suggests_decode_corruption(proc.stderr or ""):
            raise FfmpegDecodeError(
                f"ffmpeg decode/corruption failure (code {proc.returncode}). "
                f"stdout={stdout!r} stderr={stderr!r}"
            )
        raise RuntimeError(
            f"ffmpeg failed (code {proc.returncode}). stdout={stdout!r} stderr={stderr!r}"
        )


def build_preview_filename(original: Path) -> str:
    """Preview is always MP4 per pilot pipeline."""
    return f"{original.stem}.mp4"


def parse_captured_at_utc(path: Path) -> str:
    """
    Convert a UTC filename stem like 2026-04-04T19-12-13Z
    into ISO UTC 2026-04-04T19:12:13Z.
    Falls back to current UTC if parsing fails.
    """
    stem = path.stem
    if stem.lower().endswith("z") and len(stem) >= 1:
        stem = stem[:-1] + "Z"
    try:
        dt = datetime.strptime(stem, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_booking_id_for_clip(settings: Settings, recorded_at: str) -> str | None:
    """
    Ask Pickle Planner which booking this clip belongs to.
    Returns booking_id or None.
    """
    url = settings.pickle_planner_match_url
    api_key = settings.pickle_planner_api_key
    api_key_header = settings.pickle_planner_api_key_header or "x-api-key"

    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(
                "Starting Pickle Planner lookup",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "url": url,
                        "attempt": attempt,
                    }
                },
            )

            response = requests.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    api_key_header: api_key,
                },
                json={"recorded_at": recorded_at},
                timeout=10,
            )

            if response.ok:
                data = response.json()
                booking_id = data.get("booking_id")

                logger.info(
                    "Pickle Planner lookup completed",
                    extra={
                        "structured": {
                            "recorded_at": recorded_at,
                            "booking_id": booking_id,
                            "attempt": attempt,
                        }
                    },
                )

                if booking_id and isinstance(booking_id, str):
                    return booking_id.strip()

                return None

            logger.warning(
                "Pickle Planner lookup returned non-OK response",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "status_code": response.status_code,
                        "response_text": response.text,
                        "attempt": attempt,
                    }
                },
            )

        except Exception as exc:
            logger.warning(
                "Pickle Planner request failed",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "error": str(exc),
                        "attempt": attempt,
                    }
                },
            )

        if attempt < max_attempts:
            time.sleep(2 * attempt)

    return None


def _deterministic_slug(stem: str, idempotency_key: str) -> str:
    base = slug_from_stem(stem)
    return f"{base}-{idempotency_key[:12]}"


def process_clip(
    clip_path: Path,
    settings: Settings,
    primary_uploader: S3Uploader,
    supabase: Client,
    job_store: JobStore,
) -> None:
    job_store.init_schema()
    incoming = settings.clips_incoming_folder.resolve(strict=False)
    processing = settings.clips_processing_folder.resolve(strict=False)

    original_input_path = clip_path.resolve(strict=False)
    clip_path = original_input_path
    parent = clip_path.parent.resolve(strict=False)

    if not is_video_file(clip_path, settings):
        logger.info("Skipping non-video file", extra={"structured": {"path": str(clip_path)}})
        return

    if should_ignore_file(clip_path, settings):
        logger.info(
            "Ignoring file by configured name/pattern",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    if is_copying_temp_clip(clip_path, settings):
        logger.debug(
            "Skipping ingest temp file (.copying)",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    if parent not in (incoming, processing):
        logger.info(
            "Skipping file outside incoming/processing folders",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    if not clip_path.exists() or not clip_path.is_file():
        logger.info(
            "Skipping absent clip path (likely already renamed or moved)",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    if not _claim_path(clip_path):
        logger.info(
            "Skipping clip already in-flight (single-flight dedupe)",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    idem: str | None = None
    logger.info("Processing clip", extra={"structured": {"path": str(clip_path)}})

    pipeline_db_persisted = False
    pipeline_upload_started = False

    try:
        if parent == processing:
            job = job_store.ensure_job_for_processing_file(clip_path)
            idem = job.idempotency_key
            if job.status == "completed":
                logger.info(
                    "Job already finalized in DB; skipping",
                    extra={"structured": {"idempotency_key": idem}},
                )
                return
        else:
            if is_recently_completed_clip(clip_path):
                logger.info(
                    "Skipping recently completed clip (duplicate watcher/submit)",
                    extra={"structured": {"path": str(clip_path)}},
                )
                return

            if _is_in_recent_failure_cooldown(clip_path, settings):
                logger.info(
                    "Skipping clip during recent-failure cooldown",
                    extra={
                        "structured": {
                            "path": str(clip_path),
                            "cooldown_seconds": settings.recent_failure_cooldown_seconds,
                        }
                    },
                )
                return

            try:
                if not clip_readiness_gate(clip_path, settings):
                    _mark_recent_failure(clip_path)
                    logger.info(
                        "Deferred clip: readiness gate (leave for retry)",
                        extra={"structured": {"path": str(clip_path)}},
                    )
                    return

                wait_until_stable(clip_path, settings)
                _clear_recent_failure(clip_path)
            except FileNotFoundError:
                logger.info(
                    "Clip no longer present when processing started; skipping duplicate watcher event",
                    extra={"structured": {"path": str(clip_path)}},
                )
                return
            except FileLockedError:
                _mark_recent_failure(clip_path)
                if settings.locked_file_requeue_delay_seconds > 0:
                    time.sleep(settings.locked_file_requeue_delay_seconds)
                logger.info(
                    "Clip still locked by another process; leaving in place for later retry",
                    extra={"structured": {"path": str(clip_path)}},
                )
                return
            except FileStillChangingError:
                _mark_recent_failure(clip_path)
                logger.info(
                    "Clip still changing; leaving in place for later retry",
                    extra={"structured": {"path": str(clip_path)}},
                )
                return

            st = clip_path.stat()
            idem = make_idempotency_key(clip_path.name, st.st_size)
            prior = job_store.get(idem)
            if prior is not None and prior.status == "completed":
                logger.info(
                    "Idempotent skip: asset already completed",
                    extra={"structured": {"idempotency_key": idem}},
                )
                return
            if prior is not None and prior.status == "processing":
                logger.warning(
                    "Same idempotency key already has an active job; skipping duplicate file",
                    extra={"structured": {"idempotency_key": idem}},
                )
                return

            dest = processing / clip_path.name
            if dest.exists():
                dest = unique_destination(processing, clip_path.name)
            try:
                clip_path.rename(dest)
            except OSError as exc:
                logger.warning(
                    "Atomic claim rename failed",
                    extra={"structured": {"from": str(clip_path), "error": str(exc)}},
                )
                return

            clip_path = dest.resolve(strict=False)
            _release_path(original_input_path)
            if not _claim_path(clip_path):
                return
            original_input_path = clip_path

            try:
                job = job_store.insert_after_claim(
                    idempotency_key=idem,
                    incoming_basename=clip_path.name,
                    processing_path=clip_path,
                    file_size=st.st_size,
                )
            except JobIdempotencyCollisionError:
                logger.warning(
                    "Job row collision after rename; leaving file in processing for recovery",
                    extra={"structured": {"idempotency_key": idem}},
                )
                return

        assert idem is not None
        job = job_store.get(idem)
        assert job is not None

        flags = job.step_flags

        if not (flags & STEP_RENAMED_UTC):
            try:
                clip_path = rename_clip_to_utc_filename(
                    clip_path,
                    settings=settings,
                    local_tz_name=settings.local_timezone,
                    retries=settings.move_retries,
                    delay_seconds=settings.move_retry_delay_seconds,
                )
            except FileNotFoundError:
                logger.info(
                    "Clip missing before UTC rename; skipping (no further retries)",
                    extra={"structured": {"path": str(clip_path)}},
                )
                return
            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_RENAMED_UTC,
                utc_filename=clip_path.name,
                processing_path=str(clip_path.resolve(strict=False)),
            )
            flags |= STEP_RENAMED_UTC
        else:
            clip_path = Path(job.processing_path).resolve(strict=False)
            if not clip_path.is_file():
                logger.warning(
                    "Resume: processing path missing on disk",
                    extra={"structured": {"path": str(clip_path), "idempotency_key": idem}},
                )
                return

        original_name = clip_path.name
        preview_name = (
            job.preview_relpath
            if job.preview_relpath
            else build_preview_filename(clip_path)
        )
        preview_path = (settings.preview_folder / preview_name).resolve(strict=False)
        captured_at_utc = (
            job.recorded_at
            if job.recorded_at
            else parse_captured_at_utc(clip_path)
        )

        if not (flags & STEP_PREVIEW):
            try:
                run_ffmpeg_preview(settings, clip_path, preview_path)
            except FfmpegDecodeError as exc:
                key = _path_key(clip_path)
                with _FFMPEG_SOFT_FAILS_LOCK:
                    n = _FFMPEG_SOFT_FAILS.get(key, 0) + 1
                    _FFMPEG_SOFT_FAILS[key] = n
                max_soft = settings.ffmpeg_decode_max_soft_fails

                if n < max_soft:
                    logger.warning(
                        "ffmpeg decode/corruption failure; deferring clip for retry",
                        extra={
                            "structured": {
                                "path": str(clip_path),
                                "attempt": n,
                                "max_soft_fails": max_soft,
                                "error": str(exc)[:500],
                            }
                        },
                    )
                    _mark_recent_failure(clip_path)
                    rd = settings.ffmpeg_decode_retry_delay_seconds
                    if rd > 0:
                        time.sleep(rd)
                    return

                with _FFMPEG_SOFT_FAILS_LOCK:
                    _FFMPEG_SOFT_FAILS.pop(key, None)

                logger.error(
                    "ffmpeg decode failed after repeated attempts; moving to failed if possible",
                    extra={
                        "structured": {
                            "path": str(clip_path),
                            "attempts": n,
                            "error": str(exc)[:500],
                        }
                    },
                )

                if is_file_locked(clip_path):
                    logger.warning(
                        "Failed clip still locked after decode errors; leaving in processing for retry",
                        extra={"structured": {"path": str(clip_path)}},
                    )
                    return

                if not _owns_active_processing_claim(original_input_path):
                    return

                try:
                    dest = unique_destination(settings.failed_folder, clip_path.name)
                    moved_fail = move_if_exists(
                        clip_path,
                        dest,
                        retries=settings.move_retries,
                        delay_seconds=settings.move_retry_delay_seconds,
                        context="failed_after_ffmpeg_decode",
                    )
                    if moved_fail is not None:
                        job_store.update_job(
                            idem,
                            status="failed",
                            last_error="ffmpeg_decode",
                        )
                        logger.info(
                            "Moved clip to failed after repeated ffmpeg decode errors",
                            extra={"structured": {"path": str(moved_fail)}},
                        )
                except Exception:
                    logger.exception(
                        "Could not move decode-failed clip to failed folder",
                        extra={"structured": {"path": str(clip_path)}},
                    )
                return

            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_PREVIEW,
                preview_relpath=preview_name,
            )
            flags |= STEP_PREVIEW
            _clear_ffmpeg_soft_fails(clip_path)

        s3_original_key, s3_preview_key = build_s3_keys(settings, original_name, preview_name)

        job = job_store.get(idem)
        assert job is not None
        slug = job.slug or _deterministic_slug(clip_path.stem, idem)
        if not job.slug:
            job_store.update_job(idem, slug=slug)

        if not (flags & STEP_UPLOAD_ORIGINAL):
            pipeline_upload_started = True
            primary_uploader.upload_file(clip_path, s3_original_key)
            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_UPLOAD_ORIGINAL,
                s3_original_key=s3_original_key,
            )
            flags |= STEP_UPLOAD_ORIGINAL

        if not (flags & STEP_UPLOAD_PREVIEW):
            pipeline_upload_started = True
            primary_uploader.upload_file(preview_path, s3_preview_key)
            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_UPLOAD_PREVIEW,
                s3_preview_key=s3_preview_key,
            )
            flags |= STEP_UPLOAD_PREVIEW

        title = clip_path.stem
        if not (flags & STEP_DB_UPSERT):
            inserted_clip = upsert_clip_record(
                supabase,
                settings,
                title=title,
                slug=slug,
                s3_key=s3_original_key,
                preview_s3_key=s3_preview_key,
                recorded_at=captured_at_utc,
            )
            pipeline_db_persisted = True
            clip_id_val = inserted_clip.get("id")
            clip_id_str = str(clip_id_val) if clip_id_val is not None else None
            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_DB_UPSERT,
                clip_id=clip_id_str,
                recorded_at=captured_at_utc,
            )
            flags |= STEP_DB_UPSERT
        else:
            inserted_clip = {"id": job.clip_id}
            pipeline_db_persisted = True
            flags |= STEP_DB_UPSERT

        clip_id = inserted_clip.get("id")
        if not (flags & STEP_BOOKING):
            if clip_id:
                booking_id = get_booking_id_for_clip(settings, captured_at_utc)

                if booking_id:
                    try:
                        update_clip_booking_id(
                            supabase,
                            settings,
                            clip_id=str(clip_id),
                            booking_id=booking_id,
                        )
                        logger.info(
                            "Clip matched to booking",
                            extra={
                                "structured": {
                                    "clip_id": clip_id,
                                    "booking_id": booking_id,
                                    "recorded_at": captured_at_utc,
                                }
                            },
                        )
                    except Exception:
                        logger.exception(
                            "Failed to update clip with booking_id",
                            extra={
                                "structured": {
                                    "clip_id": clip_id,
                                    "booking_id": booking_id,
                                    "recorded_at": captured_at_utc,
                                }
                            },
                        )
                else:
                    logger.warning(
                        "UNMATCHED CLIP",
                        extra={
                            "structured": {
                                "clip_id": clip_id,
                                "recorded_at": captured_at_utc,
                            }
                        },
                    )
            else:
                logger.warning(
                    "Clip row has no id; skipping booking lookup",
                    extra={"structured": {"slug": slug, "recorded_at": captured_at_utc}},
                )
            job_store.update_job(idem, merge_steps=True, step_flags=STEP_BOOKING)
            flags |= STEP_BOOKING

        if not (flags & STEP_FINALIZED):
            dest = unique_destination(settings.processed_folder, original_name)
            if settings.recent_completed_suppress_seconds > 0:
                mark_clip_recently_completed(clip_path, settings.recent_completed_suppress_seconds)
            moved_ok = move_if_exists(
                clip_path,
                dest,
                retries=settings.move_retries,
                delay_seconds=settings.move_retry_delay_seconds,
                context="processed",
            )

            logger.info(
                "Clip processed successfully",
                extra={
                    "structured": {
                        "original": str(moved_ok or dest),
                        "preview": str(preview_path),
                        "slug": slug,
                        "recorded_at": captured_at_utc,
                        "moved_to_processed": moved_ok is not None,
                        "idempotency_key": idem,
                    }
                },
            )

            job_store.update_job(
                idem,
                merge_steps=True,
                step_flags=STEP_FINALIZED,
                status="completed",
                processing_path=str((moved_ok or dest).resolve(strict=False)),
            )

        _clear_recent_failure(original_input_path)
        _clear_recent_failure(clip_path)

    except FileNotFoundError:
        logger.info(
            "Processing aborted: expected file missing",
            extra={"structured": {"path": str(clip_path)}},
        )
        return
    except Exception:
        _mark_recent_failure(original_input_path)
        _mark_recent_failure(clip_path)

        if idem is not None:
            job_store.update_job(
                idem,
                last_error="processing_exception",
            )

        if pipeline_db_persisted or pipeline_upload_started:
            logger.exception(
                "Clip processing failed after upload/DB started; not moving to failed (avoid races)",
                extra={"structured": {"path": str(clip_path)}},
            )
            return

        logger.exception(
            "Clip processing failed",
            extra={"structured": {"path": str(clip_path)}},
        )

        if not _owns_active_processing_claim(original_input_path):
            logger.warning(
                "Skipping generic failed-folder move (not active processing owner)",
                extra={"structured": {"path": str(clip_path)}},
            )
            return

        try:
            if clip_path.exists() and clip_path.is_file():
                if is_file_locked(clip_path):
                    logger.warning(
                        "Failed clip is still locked; leaving in place instead of moving to failed",
                        extra={"structured": {"path": str(clip_path)}},
                    )
                else:
                    dest = unique_destination(settings.failed_folder, clip_path.name)
                    moved_fail = move_if_exists(
                        clip_path,
                        dest,
                        retries=settings.move_retries,
                        delay_seconds=settings.move_retry_delay_seconds,
                        context="failed_generic",
                    )
                    if moved_fail is not None and idem is not None:
                        job_store.update_job(
                            idem,
                            status="failed",
                            processing_path=str(moved_fail.resolve(strict=False)),
                        )
                        logger.info(
                            "Moved failed clip",
                            extra={"structured": {"path": str(moved_fail)}},
                        )
        except Exception:
            logger.exception(
                "Could not move failed clip to failed folder",
                extra={"structured": {"path": str(clip_path)}},
            )
        raise
    finally:
        _release_path(original_input_path)
        if clip_path != original_input_path:
            _release_path(clip_path)