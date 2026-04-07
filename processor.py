"""
End-to-end processing for a single clip: stabilize, rename to UTC, preview, upload, DB, move originals.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from supabase import Client

from config import Settings, slug_from_stem
from database import insert_clip_record, update_clip_booking_id
from uploader import S3Uploader

logger = logging.getLogger(__name__)

_ACTIVE_PATHS_LOCK = threading.Lock()
_ACTIVE_PATHS: set[str] = set()


def _path_key(path: Path) -> str:
    return str(path.resolve(strict=False)).lower()


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


def is_video_file(path: Path, settings: Settings) -> bool:
    return path.suffix.lower() in settings.video_extensions


def wait_until_stable(path: Path, settings: Settings) -> None:
    for round_idx in range(1, settings.file_stable_retries + 1):
        if not path.exists() or path.is_dir():
            raise FileNotFoundError(f"Clip not found (yet): {path}")

        size_before = path.stat().st_size
        time.sleep(settings.file_stable_check_seconds)

        if not path.exists() or path.is_dir():
            raise FileNotFoundError(f"Clip disappeared during stabilization: {path}")

        size_after = path.stat().st_size
        if size_before == size_after:
            logger.info(
                "File stabilized",
                extra={
                    "structured": {
                        "path": str(path),
                        "size_bytes": size_after,
                        "round": round_idx,
                    }
                },
            )
            return

        logger.debug(
            "File still growing",
            extra={
                "structured": {
                    "path": str(path),
                    "before": size_before,
                    "after": size_after,
                    "round": round_idx,
                }
            },
        )

    raise TimeoutError(
        f"File did not stabilize within {settings.file_stable_retries} checks: {path}"
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


def convert_local_filename_to_utc_name(path: Path, local_tz_name: str) -> str:
    """
    Convert a local-time OBS filename like:
        2026-04-04T14-12-13.mp4
    into a UTC filename like:
        2026-04-04T19-12-13Z.mp4

    Preserves the original file extension.
    """
    stem = path.stem
    suffix = path.suffix.lower()

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
    local_tz_name: str,
    retries: int,
    delay_seconds: float,
) -> Path:
    """
    Rename a fully written local clip file from local-time OBS naming to UTC naming.
    """
    new_name = convert_local_filename_to_utc_name(clip_path, local_tz_name=local_tz_name)

    if clip_path.name == new_name:
        return clip_path

    dest = unique_destination(clip_path.parent, new_name)
    renamed = move_with_retries(
        clip_path,
        dest,
        retries=retries,
        delay_seconds=delay_seconds,
    )

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


def build_scott_keys(settings: Settings, preview_name: str, meta_name: str) -> tuple[str, str]:
    preview_prefix = settings.scott_s3_preview_prefix.strip("/")
    meta_prefix = settings.scott_s3_meta_prefix.strip("/")

    preview_key = f"{preview_prefix}/{preview_name}" if preview_prefix else preview_name
    meta_key = f"{meta_prefix}/{meta_name}" if meta_prefix else meta_name

    return preview_key, meta_key


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
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "")[-4000:]
        stdout = (proc.stdout or "")[-2000:]
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


def write_scott_metadata_file(
    metadata_path: Path,
    *,
    filename: str,
    captured_at_utc: str,
    club_id: str,
    court_id: str,
    slug: str,
) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "filename": filename,
        "captured_at_utc": captured_at_utc,
        "club_id": club_id,
        "court_id": court_id,
        "source": "ReplayTrove",
        "slug": slug,
    }

    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def process_clip(
    clip_path: Path,
    settings: Settings,
    primary_uploader: S3Uploader,
    scott_uploader: S3Uploader,
    supabase: Client,
) -> None:
    original_input_path = clip_path.resolve(strict=False)
    clip_path = original_input_path

    if not is_video_file(clip_path, settings):
        logger.info("Skipping non-video file", extra={"structured": {"path": str(clip_path)}})
        return

    if clip_path.parent.resolve(strict=False) != settings.clips_folder.resolve(strict=False):
        logger.info(
            "Skipping file outside clips folder",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    if not _claim_path(clip_path):
        logger.info(
            "Skipping clip already being processed",
            extra={"structured": {"path": str(clip_path)}},
        )
        return

    logger.info("Processing clip", extra={"structured": {"path": str(clip_path)}})

    try:
        try:
            wait_until_stable(clip_path, settings)
        except FileNotFoundError:
            logger.info(
                "Clip no longer present when processing started; skipping duplicate watcher event",
                extra={"structured": {"path": str(clip_path)}},
            )
            return

        clip_path = rename_clip_to_utc_filename(
            clip_path,
            local_tz_name=settings.local_timezone,
            retries=settings.move_retries,
            delay_seconds=settings.move_retry_delay_seconds,
        )

        original_name = clip_path.name
        preview_name = build_preview_filename(clip_path)
        preview_path = (settings.preview_folder / preview_name).resolve(strict=False)

        meta_name = f"{clip_path.stem}.json"
        meta_path = (settings.preview_folder / meta_name).resolve(strict=False)

        captured_at_utc = parse_captured_at_utc(clip_path)

        run_ffmpeg_preview(settings, clip_path, preview_path)

        s3_original_key, s3_preview_key = build_s3_keys(settings, original_name, preview_name)

        primary_uploader.upload_file(clip_path, s3_original_key)
        primary_uploader.upload_file(preview_path, s3_preview_key)

        stem_slug = slug_from_stem(clip_path.stem)
        slug = f"{stem_slug}-{uuid.uuid4().hex[:10]}"
        title = clip_path.stem

        inserted_clip = insert_clip_record(
            supabase,
            settings,
            title=title,
            slug=slug,
            s3_key=s3_original_key,
            preview_s3_key=s3_preview_key,
            recorded_at=captured_at_utc,
        )

        clip_id = inserted_clip.get("id")
        if clip_id:
            booking_id = get_booking_id_for_clip(settings, captured_at_utc)

            if booking_id:
                try:
                    update_clip_booking_id(
                        supabase,
                        settings,
                        clip_id=clip_id,
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
                "Inserted clip row did not return an id; skipping booking lookup",
                extra={
                    "structured": {
                        "slug": slug,
                        "recorded_at": captured_at_utc,
                    }
                },
            )

        write_scott_metadata_file(
            meta_path,
            filename=preview_name,
            captured_at_utc=captured_at_utc,
            club_id=settings.club_id,
            court_id=settings.court_id,
            slug=slug,
        )

        try:
            scott_preview_key, scott_meta_key = build_scott_keys(settings, preview_name, meta_name)
            scott_uploader.upload_file(preview_path, scott_preview_key)
            scott_uploader.upload_file(meta_path, scott_meta_key)
            logger.info(
                "Scott handoff completed",
                extra={
                    "structured": {
                        "preview_key": scott_preview_key,
                        "meta_key": scott_meta_key,
                    }
                },
            )
        except Exception:
            logger.exception(
                "Scott handoff failed (non-fatal)",
                extra={"structured": {"path": str(clip_path)}},
            )

        dest = unique_destination(settings.processed_folder, original_name)
        move_with_retries(
            clip_path,
            dest,
            retries=settings.move_retries,
            delay_seconds=settings.move_retry_delay_seconds,
        )

        logger.info(
            "Clip processed successfully",
            extra={
                "structured": {
                    "original": str(dest),
                    "preview": str(preview_path),
                    "meta": str(meta_path),
                    "slug": slug,
                    "recorded_at": captured_at_utc,
                }
            },
        )

    except Exception:
        logger.exception(
            "Clip processing failed",
            extra={"structured": {"path": str(clip_path)}},
        )
        try:
            if clip_path.exists() and clip_path.is_file():
                dest = unique_destination(settings.failed_folder, clip_path.name)
                move_with_retries(
                    clip_path,
                    dest,
                    retries=settings.move_retries,
                    delay_seconds=settings.move_retry_delay_seconds,
                )
                logger.info(
                    "Moved failed clip",
                    extra={"structured": {"path": str(dest)}},
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