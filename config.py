"""
Load and validate worker settings from environment variables (.env supported via python-dotenv).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import FrozenSet, Tuple

from dotenv import load_dotenv


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid."""


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing or empty required environment variable: {name}")
    return value


def _optional(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or not str(value).strip():
        return default
    return str(value).strip()


def _parse_extensions(raw: str) -> FrozenSet[str]:
    """Parse comma-separated extensions; normalize to lowercase with leading dot."""
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    out: set[str] = set()
    for p in parts:
        if not p.startswith("."):
            p = "." + p
        out.add(p)
    if not out:
        raise ConfigError("VIDEO_EXTENSIONS must list at least one extension")
    return frozenset(out)


def _parse_bool(raw: str, default: bool = False) -> bool:
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    return default


def _parse_int(name: str, raw: str, minimum: int | None = None) -> int:
    try:
        n = int(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if minimum is not None and n < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return n


def _parse_float(name: str, raw: str, minimum: float | None = None) -> float:
    try:
        n = float(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if minimum is not None and n < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return n


def _parse_csv_strings(raw: str) -> Tuple[str, ...]:
    """Parse comma-separated strings into a tuple, preserving case."""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return tuple(parts)


_SLUG_SAFE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for the ReplayTrove clip worker."""

    clips_incoming_folder: Path
    clips_processing_folder: Path
    job_db_path: Path
    preview_folder: Path
    processed_folder: Path
    failed_folder: Path
    log_folder: Path

    ffmpeg_path: Path

    aws_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_bucket: str
    s3_original_prefix: str
    s3_preview_prefix: str

    scott_aws_region: str
    scott_aws_access_key_id: str
    scott_aws_secret_access_key: str
    scott_s3_bucket: str
    scott_s3_preview_prefix: str
    scott_s3_meta_prefix: str

    supabase_url: str
    supabase_key: str
    supabase_clips_table: str

    pickle_planner_match_url: str
    pickle_planner_api_key: str
    pickle_planner_api_key_header: str

    local_timezone: str

    club_id: str
    court_id: str

    video_extensions: FrozenSet[str]

    preview_width: int
    preview_crf: int
    preview_preset: str

    file_stable_check_seconds: float
    file_stable_retries: int
    file_stable_min_age_seconds: float

    upload_retries: int
    upload_retry_delay_seconds: float

    s3_multipart_threshold_bytes: int
    s3_multipart_chunksize_bytes: int

    move_retries: int
    move_retry_delay_seconds: float

    recent_failure_cooldown_seconds: float
    locked_file_requeue_delay_seconds: float

    ignore_filenames: Tuple[str, ...]
    ignore_prefixes: Tuple[str, ...]
    ignore_suffixes: Tuple[str, ...]

    published: bool

    instant_replay_source: Path | None
    long_clips_folder: Path | None
    long_clip_stable_seconds: float

    instant_replay_post_copy_delay_seconds: float
    clip_readiness_stable_rounds: int
    clip_readiness_max_cycles: int
    ffmpeg_decode_max_soft_fails: int
    ffmpeg_decode_retry_delay_seconds: float

    recent_completed_suppress_seconds: float

    instant_replay_source_min_age_seconds: float
    instant_replay_source_check_seconds: float
    instant_replay_source_retries: int

    instant_replay_trigger_file: Path | None
    instant_replay_trigger_settle_seconds: float
    long_clips_trigger_file: Path | None
    long_clips_scan_interval_seconds: float


def load_settings(env_file: Path | None = None) -> Settings:
    """
    Load settings from the environment.

    If ``env_file`` is provided, load that file first; otherwise ``load_dotenv()``
    searches for a ``.env`` in the current working directory.
    """
    if env_file is not None:
        load_dotenv(dotenv_path=env_file, override=False)
    else:
        load_dotenv(override=False)

    incoming = Path(_optional("WATCH_FOLDER", r"C:\ReplayTrove\clips"))
    inc_raw = os.environ.get("CLIPS_INCOMING_FOLDER", "").strip()
    if inc_raw:
        incoming = Path(inc_raw)
    else:
        clips_raw = os.environ.get("CLIPS_FOLDER", "").strip()
        if clips_raw:
            incoming = Path(clips_raw)

    proc_raw = os.environ.get("PROCESSING_CLIPS_FOLDER", "").strip()
    if proc_raw:
        processing = Path(proc_raw)
    else:
        processing = incoming.parent / "clips_processing"

    preview = Path(_optional("PREVIEW_FOLDER", r"C:\ReplayTrove\previews"))
    processed = Path(_optional("PROCESSED_FOLDER", r"C:\ReplayTrove\processed"))
    failed = Path(_optional("FAILED_FOLDER", r"C:\ReplayTrove\failed"))
    logs = Path(_optional("LOG_FOLDER", r"C:\ReplayTrove\logs"))
    job_db = Path(
        _optional("WORKER_JOB_DB", str(logs / "replaytrove_jobs.sqlite"))
    )

    ffmpeg = Path(_optional("FFMPEG_PATH", r"C:\ffmpeg\bin\ffmpeg.exe"))

    region = _require("AWS_REGION")
    key_id = _require("AWS_ACCESS_KEY_ID")
    secret = _require("AWS_SECRET_ACCESS_KEY")
    bucket = _require("S3_BUCKET")

    orig_prefix = _optional("S3_ORIGINAL_PREFIX", "originals").strip("/")
    prev_prefix = _optional("S3_PREVIEW_PREFIX", "previews").strip("/")

    scott_aws_region = _optional("SCOTT_AWS_REGION", "us-east-1")
    scott_aws_access_key_id = _require("SCOTT_AWS_ACCESS_KEY_ID")
    scott_aws_secret_access_key = _require("SCOTT_AWS_SECRET_ACCESS_KEY")
    scott_s3_bucket = _require("SCOTT_S3_BUCKET")
    scott_s3_preview_prefix = _optional("SCOTT_S3_PREVIEW_PREFIX", "replay-trove").strip("/")
    scott_s3_meta_prefix = _optional("SCOTT_S3_META_PREFIX", "replay-trove-meta").strip("/")

    local_timezone = _optional("LOCAL_TIMEZONE", "America/Chicago")

    sb_url = _require("SUPABASE_URL")
    sb_key = _require("SUPABASE_KEY")
    clips_table = _optional("SUPABASE_CLIPS_TABLE", "clips")

    pp_match_url = _require("PICKLE_PLANNER_MATCH_URL")
    pp_api_key = _require("PICKLE_PLANNER_API_KEY")
    pp_api_key_header = _optional("PICKLE_PLANNER_API_KEY_HEADER", "x-api-key")

    club = _require("CLUB_ID")
    court = _require("COURT_ID")

    exts = _parse_extensions(_optional("VIDEO_EXTENSIONS", ".mp4,.mov"))

    preview_width = _parse_int("PREVIEW_WIDTH", _optional("PREVIEW_WIDTH", "640"), minimum=16)
    preview_crf = _parse_int("PREVIEW_CRF", _optional("PREVIEW_CRF", "30"), minimum=0)
    preview_preset = _optional("PREVIEW_PRESET", "fast")

    stable_sec = _parse_float(
        "FILE_STABLE_CHECK_SECONDS",
        _optional("FILE_STABLE_CHECK_SECONDS", "2"),
        minimum=0.0001,
    )
    stable_retries = _parse_int(
        "FILE_STABLE_RETRIES",
        _optional("FILE_STABLE_RETRIES", "30"),
        minimum=1,
    )
    stable_min_age = _parse_float(
        "FILE_STABLE_MIN_AGE_SECONDS",
        _optional("FILE_STABLE_MIN_AGE_SECONDS", "8"),
        minimum=0,
    )

    up_retries = _parse_int(
        "UPLOAD_RETRIES",
        _optional("UPLOAD_RETRIES", "3"),
        minimum=1,
    )
    up_delay = _parse_float(
        "UPLOAD_RETRY_DELAY_SECONDS",
        _optional("UPLOAD_RETRY_DELAY_SECONDS", "3"),
        minimum=0,
    )

    mp_thresh = _parse_int(
        "S3_MULTIPART_THRESHOLD_BYTES",
        _optional("S3_MULTIPART_THRESHOLD_BYTES", str(32 * 1024 * 1024)),
        minimum=8 * 1024 * 1024,
    )
    mp_chunk = _parse_int(
        "S3_MULTIPART_CHUNKSIZE_BYTES",
        _optional("S3_MULTIPART_CHUNKSIZE_BYTES", str(128 * 1024 * 1024)),
        minimum=8 * 1024 * 1024,
    )

    move_retries = _parse_int(
        "MOVE_RETRIES",
        _optional("MOVE_RETRIES", "12"),
        minimum=1,
    )
    move_delay = _parse_float(
        "MOVE_RETRY_DELAY_SECONDS",
        _optional("MOVE_RETRY_DELAY_SECONDS", "2"),
        minimum=0,
    )

    recent_failure_cooldown = _parse_float(
        "RECENT_FAILURE_COOLDOWN_SECONDS",
        _optional("RECENT_FAILURE_COOLDOWN_SECONDS", "120"),
        minimum=0,
    )
    locked_file_requeue_delay = _parse_float(
        "LOCKED_FILE_REQUEUE_DELAY_SECONDS",
        _optional("LOCKED_FILE_REQUEUE_DELAY_SECONDS", "10"),
        minimum=0,
    )

    ignore_filenames = _parse_csv_strings(
        _optional("IGNORE_FILENAMES", "InstantReplay.mp4")
    )
    ignore_prefixes = _parse_csv_strings(
        _optional("IGNORE_PREFIXES", "~,.")
    )
    ignore_suffixes = _parse_csv_strings(
        _optional("IGNORE_SUFFIXES", ".tmp,.part,.partial")
    )

    published = _parse_bool(_optional("PUBLISHED", "true"))

    if "INSTANT_REPLAY_SOURCE" in os.environ:
        ir = os.environ["INSTANT_REPLAY_SOURCE"].strip()
        instant_replay_source = Path(ir) if ir else None
    else:
        instant_replay_source = Path(r"C:\ReplayTrove\INSTANTREPLAY.mp4")

    if "LONG_CLIPS_FOLDER" in os.environ:
        lc = os.environ["LONG_CLIPS_FOLDER"].strip()
        long_clips_folder = Path(lc) if lc else None
    else:
        long_clips_folder = Path(r"C:\ReplayTrove\long_clips")

    long_clip_stable_seconds = _parse_float(
        "LONG_CLIP_STABLE_SECONDS",
        _optional("LONG_CLIP_STABLE_SECONDS", "300"),
        minimum=1,
    )

    instant_replay_post_copy_delay = _parse_float(
        "INSTANT_REPLAY_POST_COPY_DELAY_SECONDS",
        _optional("INSTANT_REPLAY_POST_COPY_DELAY_SECONDS", "4"),
        minimum=0,
    )
    clip_readiness_rounds = _parse_int(
        "CLIP_READINESS_STABLE_ROUNDS",
        _optional("CLIP_READINESS_STABLE_ROUNDS", "2"),
        minimum=1,
    )
    clip_readiness_cycles = _parse_int(
        "CLIP_READINESS_MAX_CYCLES",
        _optional("CLIP_READINESS_MAX_CYCLES", "12"),
        minimum=1,
    )
    ffmpeg_decode_max_soft = _parse_int(
        "FFMPEG_DECODE_MAX_SOFT_FAILS",
        _optional("FFMPEG_DECODE_MAX_SOFT_FAILS", "3"),
        minimum=1,
    )
    ffmpeg_decode_retry_delay = _parse_float(
        "FFMPEG_DECODE_RETRY_DELAY_SECONDS",
        _optional("FFMPEG_DECODE_RETRY_DELAY_SECONDS", "5"),
        minimum=0,
    )

    recent_completed_suppress = _parse_float(
        "RECENT_COMPLETED_SUPPRESS_SECONDS",
        _optional("RECENT_COMPLETED_SUPPRESS_SECONDS", "300"),
        minimum=0,
    )

    ir_src_min_age = _parse_float(
        "INSTANT_REPLAY_SOURCE_MIN_AGE_SECONDS",
        _optional("INSTANT_REPLAY_SOURCE_MIN_AGE_SECONDS", "0.2"),
        minimum=0,
    )
    ir_src_check = _parse_float(
        "INSTANT_REPLAY_SOURCE_CHECK_SECONDS",
        _optional("INSTANT_REPLAY_SOURCE_CHECK_SECONDS", "0.4"),
        minimum=0.05,
    )
    ir_src_retries = _parse_int(
        "INSTANT_REPLAY_SOURCE_RETRIES",
        _optional("INSTANT_REPLAY_SOURCE_RETRIES", "120"),
        minimum=1,
    )

    if "INSTANT_REPLAY_TRIGGER_FILE" in os.environ:
        irt = os.environ["INSTANT_REPLAY_TRIGGER_FILE"].strip()
        instant_replay_trigger_file = Path(irt) if irt else None
    else:
        instant_replay_trigger_file = None

    instant_replay_trigger_settle = _parse_float(
        "INSTANT_REPLAY_TRIGGER_SETTLE_SECONDS",
        _optional("INSTANT_REPLAY_TRIGGER_SETTLE_SECONDS", "1.0"),
        minimum=0,
    )

    if "LONG_CLIPS_TRIGGER_FILE" in os.environ:
        lct = os.environ["LONG_CLIPS_TRIGGER_FILE"].strip()
        long_clips_trigger_file = Path(lct) if lct else None
    else:
        long_clips_trigger_file = None

    long_clips_scan_interval = _parse_float(
        "LONG_CLIPS_SCAN_INTERVAL_SECONDS",
        _optional("LONG_CLIPS_SCAN_INTERVAL_SECONDS", "10"),
        minimum=0,
    )

    return Settings(
        clips_incoming_folder=incoming,
        clips_processing_folder=processing,
        job_db_path=job_db,
        preview_folder=preview,
        processed_folder=processed,
        failed_folder=failed,
        log_folder=logs,
        ffmpeg_path=ffmpeg,
        aws_region=region,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        s3_bucket=bucket,
        s3_original_prefix=orig_prefix,
        s3_preview_prefix=prev_prefix,
        scott_aws_region=scott_aws_region,
        scott_aws_access_key_id=scott_aws_access_key_id,
        scott_aws_secret_access_key=scott_aws_secret_access_key,
        scott_s3_bucket=scott_s3_bucket,
        scott_s3_preview_prefix=scott_s3_preview_prefix,
        scott_s3_meta_prefix=scott_s3_meta_prefix,
        supabase_url=sb_url,
        supabase_key=sb_key,
        supabase_clips_table=clips_table,
        pickle_planner_match_url=pp_match_url,
        pickle_planner_api_key=pp_api_key,
        pickle_planner_api_key_header=pp_api_key_header,
        local_timezone=local_timezone,
        club_id=club,
        court_id=court,
        video_extensions=exts,
        preview_width=preview_width,
        preview_crf=preview_crf,
        preview_preset=preview_preset,
        file_stable_check_seconds=stable_sec,
        file_stable_retries=stable_retries,
        file_stable_min_age_seconds=stable_min_age,
        upload_retries=up_retries,
        upload_retry_delay_seconds=up_delay,
        s3_multipart_threshold_bytes=mp_thresh,
        s3_multipart_chunksize_bytes=mp_chunk,
        move_retries=move_retries,
        move_retry_delay_seconds=move_delay,
        recent_failure_cooldown_seconds=recent_failure_cooldown,
        locked_file_requeue_delay_seconds=locked_file_requeue_delay,
        ignore_filenames=ignore_filenames,
        ignore_prefixes=ignore_prefixes,
        ignore_suffixes=ignore_suffixes,
        published=published,
        instant_replay_source=instant_replay_source,
        long_clips_folder=long_clips_folder,
        long_clip_stable_seconds=long_clip_stable_seconds,
        instant_replay_post_copy_delay_seconds=instant_replay_post_copy_delay,
        clip_readiness_stable_rounds=clip_readiness_rounds,
        clip_readiness_max_cycles=clip_readiness_cycles,
        ffmpeg_decode_max_soft_fails=ffmpeg_decode_max_soft,
        ffmpeg_decode_retry_delay_seconds=ffmpeg_decode_retry_delay,
        recent_completed_suppress_seconds=recent_completed_suppress,
        instant_replay_source_min_age_seconds=ir_src_min_age,
        instant_replay_source_check_seconds=ir_src_check,
        instant_replay_source_retries=ir_src_retries,
        instant_replay_trigger_file=instant_replay_trigger_file,
        instant_replay_trigger_settle_seconds=instant_replay_trigger_settle,
        long_clips_trigger_file=long_clips_trigger_file,
        long_clips_scan_interval_seconds=long_clips_scan_interval,
    )


def slug_from_stem(stem: str) -> str:
    """URL-ish slug from a filename stem (lowercase, hyphen-separated)."""
    s = stem.strip().lower()
    s = _SLUG_SAFE.sub("-", s)
    s = s.strip("-")
    if not s:
        s = "clip"
    return s