from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from config import ConfigError, Settings, load_settings
from database import create_supabase_client
from ingest import (
    instant_replay_ingest_loop,
    instant_replay_trigger_loop,
    long_clips_ingest_loop,
)
from logger import setup_logging
from job_store import JobStore
from processor import is_copying_temp_clip, process_clip
from uploader import S3Uploader
from watcher import (
    ClipFileHandler,
    ClipJobQueue,
    scan_existing_clips,
    scan_processing_resume,
    start_observer,
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ReplayTrove clip worker")
    parser.add_argument(
        "--env",
        type=Path,
        default=None,
        help="Path to .env file (optional)",
    )
    return parser.parse_args(argv)


def _ensure_directories(settings: Settings) -> None:
    folders = [
        settings.clips_incoming_folder,
        settings.clips_processing_folder,
        settings.preview_folder,
        settings.processed_folder,
        settings.failed_folder,
        settings.log_folder,
    ]
    if settings.long_clips_folder is not None:
        folders.append(settings.long_clips_folder)
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)

    if settings.instant_replay_trigger_file is not None:
        p = settings.instant_replay_trigger_file
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch()
    if settings.long_clips_trigger_file is not None:
        p = settings.long_clips_trigger_file
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch()


def _warm_supabase(settings: Settings) -> None:
    client = create_supabase_client(settings)
    client.table(settings.supabase_clips_table).select("*").limit(1).execute()


def _normalize_path(path: Path) -> Path:
    return path.resolve(strict=False)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    try:
        settings = load_settings(env_file=args.env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    _ensure_directories(settings)
    setup_logging(settings.log_folder)

    logger.info(
        "Worker initialized",
        extra={
            "structured": {
                "clips_incoming_folder": str(settings.clips_incoming_folder),
                "clips_processing_folder": str(settings.clips_processing_folder),
                "job_db_path": str(settings.job_db_path),
                "preview_folder": str(settings.preview_folder),
                "processed_folder": str(settings.processed_folder),
                "failed_folder": str(settings.failed_folder),
                "log_folder": str(settings.log_folder),
                "clips_table": settings.supabase_clips_table,
                "club_id": settings.club_id,
                "court_id": settings.court_id,
                "primary_bucket": settings.s3_bucket,
                "instant_replay_source": str(settings.instant_replay_source)
                if settings.instant_replay_source
                else None,
                "long_clips_folder": str(settings.long_clips_folder)
                if settings.long_clips_folder
                else None,
                "long_clip_stable_seconds": settings.long_clip_stable_seconds,
                "instant_replay_post_copy_delay_seconds": settings.instant_replay_post_copy_delay_seconds,
                "clip_readiness_stable_rounds": settings.clip_readiness_stable_rounds,
                "clip_readiness_max_cycles": settings.clip_readiness_max_cycles,
                "ffmpeg_decode_max_soft_fails": settings.ffmpeg_decode_max_soft_fails,
                "ffmpeg_decode_retry_delay_seconds": settings.ffmpeg_decode_retry_delay_seconds,
                "recent_completed_suppress_seconds": settings.recent_completed_suppress_seconds,
                "instant_replay_source_min_age_seconds": settings.instant_replay_source_min_age_seconds,
                "instant_replay_source_check_seconds": settings.instant_replay_source_check_seconds,
                "instant_replay_source_retries": settings.instant_replay_source_retries,
                "instant_replay_trigger_file": str(settings.instant_replay_trigger_file)
                if settings.instant_replay_trigger_file
                else None,
                "instant_replay_trigger_settle_seconds": settings.instant_replay_trigger_settle_seconds,
                "long_clips_trigger_file": str(settings.long_clips_trigger_file)
                if settings.long_clips_trigger_file
                else None,
                "long_clips_scan_interval_seconds": settings.long_clips_scan_interval_seconds,
            }
        },
    )

    try:
        _warm_supabase(settings)
    except Exception:
        logger.exception("Supabase connectivity check failed")
        return 3

    supabase = create_supabase_client(settings)

    primary_uploader = S3Uploader(
        bucket=settings.s3_bucket,
        region=settings.aws_region,
        access_key_id=settings.aws_access_key_id,
        secret_access_key=settings.aws_secret_access_key,
        upload_retries=settings.upload_retries,
        upload_retry_delay_seconds=settings.upload_retry_delay_seconds,
        label="primary",
        multipart_threshold_bytes=settings.s3_multipart_threshold_bytes,
        multipart_chunksize_bytes=settings.s3_multipart_chunksize_bytes,
    )

    job_store = JobStore(settings.job_db_path)
    job_store.init_schema()

    job_queue = ClipJobQueue(settings)
    stop = threading.Event()

    def submit_job(path: Path) -> None:
        normalized = _normalize_path(path)
        if is_copying_temp_clip(normalized, settings):
            logger.debug(
                "Submit skipped: ingest temp file (.copying)",
                extra={"structured": {"path": str(normalized)}},
            )
            return
        job_queue.submit(normalized)

    def worker_loop() -> None:
        logger.info("Worker thread started")
        while not stop.is_set():
            path = job_queue.get(timeout=0.5)
            if path is None:
                continue

            try:
                process_clip(path, settings, primary_uploader, supabase, job_store)
            except Exception:
                logger.exception(
                    "Worker failed processing clip",
                    extra={"structured": {"path": str(path)}},
                )
            finally:
                try:
                    job_queue.mark_done(path)
                finally:
                    job_queue.task_done()

        logger.info("Worker thread exiting")

    worker = threading.Thread(target=worker_loop, name="clip-worker", daemon=True)
    worker.start()

    ingest_threads: list[threading.Thread] = []

    if settings.instant_replay_source is not None:

        def _instant_poll() -> None:
            try:
                instant_replay_ingest_loop(settings, submit_job, stop)
            except Exception:
                logger.exception("Instant replay poll ingest thread exited with error")

        def _instant_trigger() -> None:
            try:
                instant_replay_trigger_loop(settings, submit_job, stop)
            except Exception:
                logger.exception("Instant replay trigger ingest thread exited with error")

        if settings.instant_replay_trigger_file is not None:
            ir_thread = threading.Thread(
                target=_instant_trigger,
                name="instant-replay-trigger",
                daemon=True,
            )
        else:
            ir_thread = threading.Thread(
                target=_instant_poll,
                name="instant-replay-poll",
                daemon=True,
            )
        ir_thread.start()
        ingest_threads.append(ir_thread)

    if settings.long_clips_folder is not None:
        if (
            settings.long_clips_scan_interval_seconds <= 0
            and settings.long_clips_trigger_file is None
        ):
            logger.warning(
                "Long clips folder configured but ingest disabled "
                "(set LONG_CLIPS_SCAN_INTERVAL_SECONDS > 0 and/or LONG_CLIPS_TRIGGER_FILE)",
                extra={"structured": {"folder": str(settings.long_clips_folder)}},
            )
        else:

            def _long_ingest() -> None:
                try:
                    long_clips_ingest_loop(settings, submit_job, stop)
                except Exception:
                    logger.exception("Long clips ingest thread exited with error")

            lc_thread = threading.Thread(
                target=_long_ingest,
                name="long-clips-ingest",
                daemon=True,
            )
            lc_thread.start()
            ingest_threads.append(lc_thread)

    handler = ClipFileHandler(settings, submit_job)
    observer = start_observer(settings, handler)

    logger.info(
        "Scanning existing clips on startup",
        extra={
            "structured": {
                "clips_incoming_folder": str(settings.clips_incoming_folder),
                "clips_processing_folder": str(settings.clips_processing_folder),
            }
        },
    )
    scan_existing_clips(settings, submit_job)
    scan_processing_resume(settings, submit_job)

    def handle_signal(signum: int, _frame: object | None) -> None:
        logger.info(
            "Shutdown signal received",
            extra={"structured": {"signal": signum}},
        )
        stop.set()
        try:
            observer.stop()
        except Exception:
            logger.exception("Failed stopping observer during shutdown")

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    logger.info("ReplayTrove worker running; press Ctrl+C to stop")

    try:
        while observer.is_alive() and not stop.is_set():
            time.sleep(0.25)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down")
        stop.set()
        observer.stop()
    finally:
        stop.set()
        try:
            observer.stop()
        except Exception:
            logger.exception("Failed stopping observer in final shutdown block")

    observer.join(timeout=30)
    worker.join(timeout=10)
    for t in ingest_threads:
        t.join(timeout=5)

    logger.info("ReplayTrove worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())